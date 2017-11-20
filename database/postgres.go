package database

import (
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // Postgres database package
	"github.com/spf13/viper"

	"github.com/wtg/shuttletracker/model"
)

// Postgres implements Database with—you guessed it—Postgres.
type Postgres struct {
	db *sqlx.DB
}

// PostgresConfig contains information on how to connect to a Postgres server.
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
        color text,
        created timestamp with time zone NOT NULL DEFAULT current_timestamp,
        updated timestamp with time zone NOT NULL DEFAULT current_timestamp
    );

    CREATE TABLE IF NOT EXISTS stops (
        id serial PRIMARY KEY,
        name text,
        description text,
        latitude numeric NOT NULL,
        longitude numeric NOT NULL,
        enabled boolean NOT NULL,
        created timestamp with time zone NOT NULL DEFAULT current_timestamp,
        updated timestamp with time zone NOT NULL DEFAULT current_timestamp
    );

    --DROP TABLE routes_stops;
    CREATE TABLE IF NOT EXISTS routes_stops (
        id serial PRIMARY KEY,
        route_id integer REFERENCES routes NOT NULL,
        stop_id integer REFERENCES stops NOT NULL,
        stop_order integer NOT NULL,
        UNIQUE (route_id, stop_order)
    );

    CREATE TABLE IF NOT EXISTS vehicles (
        id serial PRIMARY KEY,  -- this is our internal ID for each vehicle
        itrak_id integer UNIQUE,  -- this is the ID that iTrak returns
        name text,
        enabled boolean NOT NULL,
        created timestamp with time zone NOT NULL DEFAULT current_timestamp,
        updated timestamp with time zone NOT NULL DEFAULT current_timestamp
    );
    CREATE INDEX IF NOT EXISTS vehicles_enabled_idx ON vehicles (enabled);

    CREATE TABLE IF NOT EXISTS updates (
        id serial PRIMARY KEY,
        vehicle_id integer REFERENCES vehicles NOT NULL,
        latitude numeric NOT NULL,
        longitude numeric NOT NULL,
        heading numeric NOT NULL,
        speed numeric NOT NULL,
        timestamp timestamp with time zone NOT NULL,
        created timestamp with time zone NOT NULL DEFAULT current_timestamp
    );
    CREATE INDEX IF NOT EXISTS updates_created_idx ON updates (created);
    CREATE INDEX IF NOT EXISTS updates_vehicle_id_created_idx ON updates (vehicle_id, created);

    CREATE TABLE IF NOT EXISTS users (
        id serial PRIMARY KEY,
        rcs_id text
    );
    `
	_, err = db.Exec(schema)

	return pg, err
}

// NewPostgresConfig creates a PostgresConfig from a Viper instance.
func NewPostgresConfig(v *viper.Viper) *PostgresConfig {
	cfg := &PostgresConfig{
		PostgresURL: "postgres://postgres@127.0.0.1:5432?sslmode=disable",
	}
	v.SetDefault("database.postgresurl", cfg.PostgresURL)
	return cfg
}

// CreateRoute creates a Route.
func (pg *Postgres) CreateRoute(route *model.Route) error {
	stmt, err := pg.db.PrepareNamed(`
        INSERT INTO routes (name, description, enabled, color)
        VALUES (:name, :description, :enabled, :color)
        RETURNING id, created, updated;`)
	if err != nil {
		return err
	}
	return stmt.Get(route, route)
}

// DeleteRoute deletes a Route by its ID.
func (pg *Postgres) DeleteRoute(routeID string) error {
	_, err := pg.db.Exec(`DELETE FROM routes WHERE id = $1;`, routeID)
	return err
}

// GetRoute returns a Route by its ID.
func (pg *Postgres) GetRoute(routeID string) (model.Route, error) {
	stmt, err := pg.db.Preparex(`SELECT * FROM routes WHERE id = $1;`)
	if err != nil {
		return model.Route{}, err
	}
	var route model.Route
	err = stmt.Get(&route, routeID)
	return route, err
}

// GetRoutes returns all Routes.
func (pg *Postgres) GetRoutes() ([]model.Route, error) {
	routes := []model.Route{}
	query := `SELECT * FROM routes;`
	err := pg.db.Select(&routes, query)
	return routes, err
}

// ModifyRoute updates an existing Route by its ID.
func (pg *Postgres) ModifyRoute(route *model.Route) error {
	stmt, err := pg.db.PrepareNamed(`
        UPDATE routes SET (name, description, enabled, color)
        = (:name, :description, :enabled, :color)
        WHERE id = :id
        RETURNING updated;`)
	if err != nil {
		return err
	}
	return stmt.Get(route, route)
}

// CreateStop creates a Stop.
func (pg *Postgres) CreateStop(stop *model.Stop) error {
	stmt, err := pg.db.PrepareNamed(`
        INSERT INTO stops (name, description, enabled)
        VALUES (:name, :description, :enabled)
        RETURNING id, created, updated;`)
	if err != nil {
		return err
	}
	return stmt.Get(stop, stop)
}

// AddStopToRoute associates a Stop with a Route.
func (pg *Postgres) AddStopToRoute(stopID string, routeID string) error {
	tx, err := pg.db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
        INSERT INTO routes_stops (route_id, stop_id, stop_order)
        VALUES ($1, $2, :stop_order)
        RETURNING id, stop_order;`)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(routeID, stopID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// DeleteStop deletes a Stop by its ID.
func (pg *Postgres) DeleteStop(stopID string) error {
	return nil
}

// GetStop returns a Stop by its ID.
func (pg *Postgres) GetStop(stopID string) (model.Stop, error) {
	var stop model.Stop
	return stop, nil
}

// GetStops returns all Stops.
func (pg *Postgres) GetStops() ([]model.Stop, error) {
	var stops []model.Stop
	return stops, nil
}

// CreateUpdate creates an Update.
func (pg *Postgres) CreateUpdate(update *model.Update) error {
	stmt, err := pg.db.PrepareNamed(`
        INSERT INTO updates (latitude, longitude, vehicle_id, heading, speed, timestamp)
        VALUES (:latitude, :longitude, :vehicle_id, :heading, :speed, :timestamp)
        RETURNING id, created;`)
	if err != nil {
		return err
	}
	return stmt.Get(update, update)
}

// DeleteUpdatesBefore deletes all Updates that were created before a time.
func (pg *Postgres) DeleteUpdatesBefore(before time.Time) (int64, error) {
	res, err := pg.db.Exec(`DELETE FROM updates WHERE created < $1;`, before)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// GetLastUpdateForVehicle returns the latest Update for a vehicle by its ID.
func (pg *Postgres) GetLastUpdateForVehicle(vehicleID int) (model.Update, error) {
	stmt, err := pg.db.Preparex(`
        SELECT * FROM updates WHERE vehicle_id = $1
        ORDER BY created DESC LIMIT 1;`)
	if err != nil {
		return model.Update{}, err
	}
	var update model.Update
	err = stmt.Get(&update, vehicleID)
	if err == sql.ErrNoRows {
		return update, ErrUpdateNotFound
	}
	return update, err
}

// GetUpdatesForVehicleSince returns all updates since a time for a vehicle by its ID.
func (pg *Postgres) GetUpdatesForVehicleSince(vehicleID int, since time.Time) ([]model.Update, error) {
	stmt, err := pg.db.Preparex(`
        SELECT * FROM updates
        WHERE vehicle_id = $1 and created > $2
        ORDER BY created DESC;`)
	if err != nil {
		return []model.Update{}, err
	}
	updates := []model.Update{}
	err = stmt.Select(&updates, vehicleID, since)
	if err == sql.ErrNoRows {
		return updates, ErrUpdateNotFound
	}
	return updates, err
}

// GetUsers returns all Users.
func (pg *Postgres) GetUsers() ([]model.User, error) {
	users := []model.User{}
	query := `SELECT * FROM users;`
	rows, err := pg.db.Queryx(query)
	if err != nil {
		return users, err
	}
	for rows.Next() {
		var user model.User
		err = rows.StructScan(&user)
		if err != nil {
			return users, err
		}
		users = append(users, user)
	}
	return users, err
}

// CreateVehicle creates a Vehicle.
func (pg *Postgres) CreateVehicle(vehicle *model.Vehicle) error {
	stmt, err := pg.db.PrepareNamed(`
        INSERT INTO vehicles (itrak_id, name, enabled)
        VALUES (:itrak_id, :name, :enabled)
        RETURNING id, created, updated;`)
	if err != nil {
		return err
	}
	return stmt.Get(vehicle, vehicle)
}

// DeleteVehicle deletes a Vehicle by its ID.
func (pg *Postgres) DeleteVehicle(vehicleID int) error {
	return nil
}

// GetVehicle returns a Vehicle by its ID.
func (pg *Postgres) GetVehicle(vehicleID int) (model.Vehicle, error) {
	stmt, err := pg.db.Preparex(`SELECT * FROM vehicles WHERE id = $1;`)
	if err != nil {
		return model.Vehicle{}, err
	}
	var vehicle model.Vehicle
	err = stmt.Get(&vehicle, vehicleID)
	if err == sql.ErrNoRows {
		return vehicle, ErrVehicleNotFound
	}
	return vehicle, err
}

// GetVehicleByITrakID returns a Vehicle by its iTrak ID.
func (pg *Postgres) GetVehicleByITrakID(itrakID int) (model.Vehicle, error) {
	stmt, err := pg.db.Preparex(`SELECT * FROM vehicles WHERE itrak_id = $1;`)
	if err != nil {
		return model.Vehicle{}, err
	}
	var vehicle model.Vehicle
	err = stmt.Get(&vehicle, itrakID)
	if err == sql.ErrNoRows {
		return vehicle, ErrVehicleNotFound
	}
	return vehicle, err
}

// GetVehicles returns all Vehicles.
func (pg *Postgres) GetVehicles() ([]model.Vehicle, error) {
	vehicles := []model.Vehicle{}
	query := `SELECT * FROM vehicles;`
	err := pg.db.Select(&vehicles, query)
	return vehicles, err
}

// GetEnabledVehicles returns all Vehicles that are enabled.
func (pg *Postgres) GetEnabledVehicles() ([]model.Vehicle, error) {
	vehicles := []model.Vehicle{}
	query := `SELECT * FROM vehicles WHERE enabled = true;`
	err := pg.db.Select(&vehicles, query)
	return vehicles, err
}

// ModifyVehicle updates a Vehicle by its ID.
func (pg *Postgres) ModifyVehicle(vehicle *model.Vehicle) error {
	stmt, err := pg.db.PrepareNamed(`
        UPDATE vehicles SET (name, itrak_id, enabled)
        = (:name, :itrak_id, :enabled)
        WHERE id = :id
        RETURNING updated;`)
	if err != nil {
		return err
	}
	err = stmt.Get(vehicle, vehicle)
	return err
}
