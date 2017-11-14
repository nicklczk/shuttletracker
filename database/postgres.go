package database

import (
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"

	"github.com/wtg/shuttletracker/model"
)

// MongoDB implements Database with—you guessed it—MongoDB.
type Postgres struct {
	db *sqlx.DB
}

// MongoDBConfig contains information on how to connect to a MongoDB server.
type PostgresConfig struct {
	PostgresURL string
}

// NewPostgres creates a Postgres database client.
func NewPostgres(cfg PostgresConfig) (*Postgres, error) {
	pg := &Postgres{}

	db, err := sqlx.Open("postgres", cfg.PostgresURL)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	pg.db = db

	schema := `
    CREATE TABLE IF NOT EXISTS routes (
        id serial PRIMARY KEY,
        name text,
        description text,
        enabled boolean NOT NULL,
        color character varying(6),
        created timestamp with time zone NOT NULL DEFAULT current_timestamp,
        updated timestamp with time zone NOT NULL DEFAULT current_timestamp
    );

    CREATE TABLE IF NOT EXISTS vehicles (
        id serial PRIMARY KEY,
        name text,
        enabled boolean NOT NULL,
        created timestamp with time zone NOT NULL DEFAULT current_timestamp,
        updated timestamp with time zone NOT NULL DEFAULT current_timestamp
    );
    CREATE INDEX vehicles_enabled_idx ON vehicles (enabled);

    CREATE TABLE IF NOT EXISTS updates (
        id serial PRIMARY KEY,
        vehicle_id integer REFERENCES vehicles,
        latitude numeric NOT NULL,
        longitude numeric NOT NULL,
        heading numeric NOT NULL,
        speed numeric NOT NULL,
        timestamp timestamp with time zone NOT NULL,
        status text NOT NULL,
        created timestamp with time zone NOT NULL DEFAULT current_timestamp
    );
    CREATE INDEX updates_created_idx ON updates (created);
    CREATE INDEX updates_vehicle_id_created_idx ON updates (vehicle_id, created);
    `
	_, err = db.Exec(schema)

	return pg, err
}

// NewPostgresConfig creates a PostgresConfig from a Viper instance.
func NewPostgresConfig(v *viper.Viper) *PostgresConfig {
	cfg := &PostgresConfig{
		PostgresURL: "127.0.0.1:5432",
	}
	v.SetDefault("database.postgresurl", cfg.PostgresURL)
	return cfg
}

// CreateRoute creates a Route.
func (pg *Postgres) CreateRoute(route *model.Route) error {
	query := `INSERT INTO routes VALUES
        (name, description, enabled, color)
        (:name, :description, :enabled, :color)
        RETURNING (id, created, updated);`
	rows, err := pg.db.NamedQuery(query, route)
	if err != nil {
		return err
	}
	for rows.Next() {
		return rows.StructScan(route)
	}
	return nil
}

/*
// DeleteRoute deletes a Route by its ID.
func (m *MongoDB) DeleteRoute(routeID string) error {
	return m.routes.Remove(bson.M{"id": routeID})
}

// GetRoute returns a Route by its ID.
func (m *MongoDB) GetRoute(routeID string) (model.Route, error) {
	var route model.Route
	err := m.routes.Find(bson.M{"id": routeID}).One(&route)
	return route, err
}

// GetRoutes returns all Routes.
func (m *MongoDB) GetRoutes() ([]model.Route, error) {
	var routes []model.Route
	err := m.routes.Find(bson.M{}).All(&routes)
	return routes, err
}

// ModifyRoute updates an existing Route by its ID.
func (m *MongoDB) ModifyRoute(route *model.Route) error {
	return m.routes.Update(bson.M{"id": route.ID}, route)
}

// CreateStop creates a Stop.
func (m *MongoDB) CreateStop(stop *model.Stop) error {
	return m.stops.Insert(&stop)
}

// DeleteStop deletes a Stop by its ID.
func (m *MongoDB) DeleteStop(stopID string) error {
	return m.stops.Remove(bson.M{"id": stopID})
}

// GetStop returns a Stop by its ID.
func (m *MongoDB) GetStop(stopID string) (model.Stop, error) {
	var stop model.Stop
	err := m.stops.Find(bson.M{"id": stopID}).One(&stop)
	return stop, err
}

// GetStops returns all Stops.
func (m *MongoDB) GetStops() ([]model.Stop, error) {
	var stops []model.Stop
	err := m.stops.Find(bson.M{}).All(&stops)
	return stops, err
}

// CreateUpdate creates an Update.
func (m *MongoDB) CreateUpdate(update *model.VehicleUpdate) error {
	return m.updates.Insert(&update)
}

// DeleteUpdatesBefore deletes all Updates that were created before a time.
func (m *MongoDB) DeleteUpdatesBefore(before time.Time) (int, error) {
	info, err := m.updates.RemoveAll(bson.M{"created": bson.M{"$lt": before}})
	if err != nil {
		return 0, err
	}
	return info.Removed, nil
}

// GetLastUpdateForVehicle returns the latest Update for a vehicle by its ID.
func (m *MongoDB) GetLastUpdateForVehicle(vehicleID string) (model.VehicleUpdate, error) {
	var update model.VehicleUpdate
	err := m.updates.Find(bson.M{"vehicleID": vehicleID}).Sort("-created").One(&update)
	return update, err
}

// GetUpdatesForVehicleSince returns all updates since a time for a vehicle by its ID.
func (m *MongoDB) GetUpdatesForVehicleSince(vehicleID string, since time.Time) ([]model.VehicleUpdate, error) {
	var updates []model.VehicleUpdate
	err := m.updates.Find(bson.M{"vehicleID": vehicleID, "created": bson.M{"$gt": since}}).Sort("-created").All(&updates)
	return updates, err
}

// GetUsers returns all Users.
func (m *MongoDB) GetUsers() ([]model.User, error) {
	var users []model.User
	err := m.users.Find(bson.M{}).All(&users)
	return users, err
}

// CreateVehicle creates a Vehicle.
func (m *MongoDB) CreateVehicle(vehicle *model.Vehicle) error {
	return m.vehicles.Insert(&vehicle)
}

// DeleteVehicle deletes a Vehicle by its ID.
func (m *MongoDB) DeleteVehicle(vehicleID string) error {
	return m.vehicles.Remove(bson.M{"vehicleID": vehicleID})
}

// GetVehicle returns a Vehicle by its ID.
func (m *MongoDB) GetVehicle(vehicleID string) (model.Vehicle, error) {
	var vehicle model.Vehicle
	err := m.vehicles.Find(bson.M{"vehicleID": vehicleID}).One(&vehicle)
	return vehicle, err
}

// GetVehicles returns all Vehicles.
func (m *MongoDB) GetVehicles() ([]model.Vehicle, error) {
	var vehicles []model.Vehicle
	err := m.vehicles.Find(bson.M{}).All(&vehicles)
	return vehicles, err
}

// GetEnabledVehicles returns all Vehicles that are enabled.
func (m *MongoDB) GetEnabledVehicles() ([]model.Vehicle, error) {
	var vehicles []model.Vehicle
	err := m.vehicles.Find(bson.M{"enabled": true}).All(&vehicles)
	return vehicles, err
}

// ModifyVehicle updates a Vehicle by its ID.
func (m *MongoDB) ModifyVehicle(vehicle *model.Vehicle) error {
	return m.vehicles.Update(bson.M{"vehicleID": vehicle.VehicleID}, vehicle)
}
*/
