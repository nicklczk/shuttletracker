package updater

import (
	"io/ioutil"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"

	"github.com/wtg/shuttletracker/database"
	"github.com/wtg/shuttletracker/log"
	"github.com/wtg/shuttletracker/model"
)

// Updater handles periodically grabbing the latest vehicle location data from iTrak.
type Updater struct {
	cfg            Config
	updateInterval time.Duration
	db             database.Database
	dataRegexp     *regexp.Regexp
}

type Config struct {
	DataFeed       string
	UpdateInterval string
}

// New creates an Updater.
func New(cfg Config, db database.Database) (*Updater, error) {
	updater := &Updater{cfg: cfg, db: db}

	interval, err := time.ParseDuration(cfg.UpdateInterval)
	if err != nil {
		return nil, err
	}
	updater.updateInterval = interval

	// Match each API field with any number (+)
	//   of the previous expressions (\d digit, \. escaped period, - negative number)
	//   Specify named capturing groups to store each field from data feed
	updater.dataRegexp = regexp.MustCompile(`(?P<id>Vehicle ID:([\d\.]+)) (?P<lat>lat:([\d\.-]+)) (?P<lng>lon:([\d\.-]+)) (?P<heading>dir:([\d\.-]+)) (?P<speed>spd:([\d\.-]+)) (?P<lock>lck:([\d\.-]+)) (?P<time>time:([\d]+)) (?P<date>date:([\d]+)) (?P<status>trig:([\d]+))`)

	return updater, nil
}

func NewConfig(v *viper.Viper) *Config {
	cfg := &Config{
		UpdateInterval: "10s",
	}
	v.SetDefault("updater.updateinterval", cfg.UpdateInterval)
	v.SetDefault("updater.datafeed", cfg.DataFeed)
	return cfg
}

// Run updater forever.
func (u *Updater) Run() {
	log.Debug("Updater started.")
	ticker := time.Tick(u.updateInterval)

	// Do one initial update.
	u.update()

	// Call update() every updateInterval.
	for range ticker {
		u.update()
	}
}

// Send a request to iTrak API, get updated shuttle info,
// store updated records in the database, and remove old records.
func (u *Updater) update() {
	// Make request to iTrak data feed
	client := http.Client{Timeout: time.Second * 5}
	resp, err := client.Get(u.cfg.DataFeed)
	if err != nil {
		log.WithError(err).Error("Could not get data feed.")
		return
	}

	// Read response body content
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.WithError(err).Error("Could not read data feed.")
		return
	}
	resp.Body.Close()

	delim := "eof"
	// split the body of response by delimiter
	vehiclesData := strings.Split(string(body), delim)
	vehiclesData = vehiclesData[:len(vehiclesData)-1] // last element is EOF

	// TODO: Figure out if this handles == 1 vehicle correctly or always assumes > 1.
	if len(vehiclesData) <= 1 {
		log.Warnf("Found no vehicles delineated by '%s'.", delim)
	}

	wg := sync.WaitGroup{}
	// for parsed data, update each vehicle
	for _, vehicleData := range vehiclesData {
		wg.Add(1)
		go func(vehicleData string) {
			defer wg.Done()
			match := u.dataRegexp.FindAllStringSubmatch(vehicleData, -1)[0]
			// Store named capturing group and matching expression as a key value pair
			result := map[string]string{}
			for i, item := range match {
				result[u.dataRegexp.SubexpNames()[i]] = item
			}

			// See if this iTrak ID is known
			vehicleIDStr := strings.Replace(result["id"], "Vehicle ID:", "", -1)
			vehicleID, err := strconv.Atoi(vehicleIDStr)
			if err != nil {
				log.WithError(err).Error("Unable to parse vehicle ID to int from string.")
				return
			}
			vehicle, err := u.db.GetVehicleByITrakID(vehicleID)
			if err == database.ErrVehicleNotFound {
				log.Warnf("Unknown iTrak vehicle ID \"%d\". Make sure all vehicles have been added.", vehicleID)
				return
			} else if err != nil {
				log.WithError(err).Error("Unable to fetch vehicle.")
				return
			}

			// Create new vehicle update & insert update into database

			// convert KPH to MPH
			speedKMH, err := strconv.ParseFloat(strings.Replace(result["speed"], "spd:", "", -1), 64)
			if err != nil {
				log.Error(err)
				return
			}
			speedMPH := kphToMPH(speedKMH)
			speedMPHString := strconv.FormatFloat(speedMPH, 'f', 5, 64)

			route := model.Route{}

			// determine if this is a new update from iTrak by comparing timestamps
			gotLastUpdate := false
			lastUpdate, err := u.db.GetLastUpdateForVehicle(vehicle.ID)
			if err != nil && err != database.ErrUpdateNotFound {
				log.WithError(err).Error("Unable to retrieve last update.")
				return
			} else if err == nil {
				gotLastUpdate = true
			}

			// turn separate time and date strings into time.Time
			itrakTime := strings.Replace(result["time"], "time:", "", -1)
			itrakDate := strings.Replace(result["date"], "date:", "", -1)
			timestamp, err := generateTimestamp(itrakTime, itrakDate)
			if err != nil {
				log.WithError(err).Error("Unable to generate update timestamp.")
				return
			}

			if gotLastUpdate && (lastUpdate.Timestamp == timestamp) {
				// Timestamp is not new; don't store update.
				return
			}
			log.Debugf("Updating %s.", vehicle.Name)

			// vehicle found and no error
			route, err = u.GuessRouteForVehicle(&vehicle)
			if err != nil {
				log.WithError(err).Error("Unable to guess route for vehicle.")
				return
			}

			latitude, err := strconv.ParseFloat(strings.Replace(result["lat"], "lat:", "", -1), 64)
			if err != nil {
				log.Error(err)
			}
			longitude, err := strconv.ParseFloat(strings.Replace(result["lng"], "lon:", "", -1), 64)
			if err != nil {
				log.Error(err)
			}

			update := model.Update{
				VehicleID: vehicle.ID,
				Latitude:  latitude,
				Longitude: longitude,
				Heading:   strings.Replace(result["heading"], "dir:", "", -1),
				Speed:     speedMPHString,
				Lock:      strings.Replace(result["lock"], "lck:", "", -1),
				Timestamp: timestamp,
				Created:   time.Now(),
				Route:     route.ID,
			}

			if err := u.db.CreateUpdate(&update); err != nil {
				log.WithError(err).Errorf("Could not insert vehicle update.")
			}
		}(vehicleData)
	}
	wg.Wait()
	log.Debugf("Updated vehicles.")

	// Prune updates older than one month
	deleted, err := u.db.DeleteUpdatesBefore(time.Now().AddDate(0, -1, 0))
	if err != nil {
		log.WithError(err).Error("Unable to remove old updates.")
		return
	}
	if deleted > 0 {
		log.Debugf("Removed %d old updates.", deleted)
	}
}

// generateTimestamp takes iTrak time and date strings and returns a time.Time representing them.
// I'm not sure if this will work in January (does iTrak return "1" or "01" for January?)
func generateTimestamp(itrakTime string, itrakDate string) (time.Time, error) {
	day, err := strconv.Atoi(itrakDate[2:4])
	if err != nil {
		return time.Time{}, err
	}
	year, err := strconv.Atoi(itrakDate[4:8])
	if err != nil {
		return time.Time{}, err
	}

	// we need to convert int to month
	monthNum, err := strconv.Atoi(itrakDate[0:2])
	if err != nil {
		return time.Time{}, err
	}
	var monthTime time.Time
	monthTime = monthTime.AddDate(0, monthNum-1, 0)
	month := monthTime.Month()

	hour, err := strconv.Atoi(itrakTime[0 : len(itrakTime)-4])
	if err != nil {
		return time.Time{}, err
	}
	minute, err := strconv.Atoi(itrakTime[len(itrakTime)-4 : len(itrakTime)-2])
	if err != nil {
		return time.Time{}, err
	}
	second, err := strconv.Atoi(itrakTime[len(itrakTime)-2 : len(itrakTime)])
	if err != nil {
		return time.Time{}, err
	}

	timestamp := time.Date(year, month, day, hour, minute, second, 0, time.UTC)
	return timestamp, nil
}

// Convert kmh to mph
func kphToMPH(kmh float64) float64 {
	return kmh * 0.621371192
}

// GuessRouteForVehicle returns a guess at what route the vehicle is on.
// It may return an empty route if it does not believe a vehicle is on any route.
func (u *Updater) GuessRouteForVehicle(vehicle *model.Vehicle) (route model.Route, err error) {
	routes, err := u.db.GetRoutes()
	if err != nil {
		log.Error(err)
	}

	routeDistances := make(map[string]float64)
	for _, route := range routes {
		routeDistances[route.ID] = 0
	}

	updates, err := u.db.GetUpdatesForVehicleSince(vehicle.ID, time.Now().Add(time.Minute*-15))
	if len(updates) < 5 {
		// Don't make a guess with fewer than 5 updates.
		log.Debugf("%v has too few recent updates (%d) to guess route.", vehicle.Name, len(updates))
		return
	}

	for _, update := range updates {
		updateLatitude := update.Latitude
		updateLongitude := update.Longitude

		for _, route := range routes {
			if !route.Enabled {
				routeDistances[route.ID] += math.Inf(0)
			}
			nearestDistance := math.Inf(0)
			for _, coord := range route.Coords {
				distance := math.Sqrt(math.Pow(updateLatitude-coord.Lat, 2) +
					math.Pow(updateLongitude-coord.Lng, 2))
				if distance < nearestDistance {
					nearestDistance = distance

				}
			}
			if nearestDistance > .003 {
				nearestDistance += 50
			}
			routeDistances[route.ID] += nearestDistance
		}
	}

	minDistance := math.Inf(0)
	var minRouteID string
	for id := range routeDistances {
		distance := routeDistances[id] / float64(len(updates))
		if distance < minDistance {
			minDistance = distance
			minRouteID = id
			// If more than ~5% of the last 100 samples were far away from a route, say the shuttle is not on a route
			// This is extremely aggressive and requires a shuttle to be on a route for ~5 minutes before it registers as on the route
			if minDistance > 5 {
				minRouteID = ""
			}
		}
	}

	// not on a route
	if minRouteID == "" {
		log.Debugf("%v not on route; distance from nearest: %v", vehicle.Name, minDistance)
		return model.Route{}, nil
	}

	route, err = u.db.GetRoute(minRouteID)
	if err != nil {
		return route, err
	}
	log.Debugf("%v on %s route.", vehicle.Name, route.Name)
	return route, err
}
