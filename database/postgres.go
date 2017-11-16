package database

import (
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // Postgres package
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

    CREATE TABLE IF NOT EXISTS vehicles (
        id serial PRIMARY KEY,
        name text,
        enabled boolean NOT NULL,
        created timestamp with time zone NOT NULL DEFAULT current_timestamp,
        updated timestamp with time zone NOT NULL DEFAULT current_timestamp
    );
    CREATE INDEX IF NOT EXISTS vehicles_enabled_idx ON vehicles (enabled);

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
	err = stmt.Get(route, route)
	return err
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
	err = stmt.Get(route, route)
	return err
}

// CreateStop creates a Stop.
func (pg *Postgres) CreateStop(stop *model.Stop) error {
	return nil
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
func (pg *Postgres) CreateUpdate(update *model.VehicleUpdate) error {
	return nil
}

// DeleteUpdatesBefore deletes all Updates that were created before a time.
func (pg *Postgres) DeleteUpdatesBefore(before time.Time) (int, error) {
	return 0, nil
}

// GetLastUpdateForVehicle returns the latest Update for a vehicle by its ID.
func (pg *Postgres) GetLastUpdateForVehicle(vehicleID string) (model.VehicleUpdate, error) {
	var update model.VehicleUpdate
	return update, nil
}

// GetUpdatesForVehicleSince returns all updates since a time for a vehicle by its ID.
func (pg *Postgres) GetUpdatesForVehicleSince(vehicleID string, since time.Time) ([]model.VehicleUpdate, error) {
	var updates []model.VehicleUpdate
	return updates, nil
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
	return nil
}

// DeleteVehicle deletes a Vehicle by its ID.
func (pg *Postgres) DeleteVehicle(vehicleID string) error {
	return nil
}

// GetVehicle returns a Vehicle by its ID.
func (pg *Postgres) GetVehicle(vehicleID string) (model.Vehicle, error) {
	var vehicle model.Vehicle
	return vehicle, nil
}

// GetVehicles returns all Vehicles.
func (pg *Postgres) GetVehicles() ([]model.Vehicle, error) {
	var vehicles []model.Vehicle
	return vehicles, nil
}

// GetEnabledVehicles returns all Vehicles that are enabled.
func (pg *Postgres) GetEnabledVehicles() ([]model.Vehicle, error) {
	var vehicles []model.Vehicle
	return vehicles, nil
}

// ModifyVehicle updates a Vehicle by its ID.
func (pg *Postgres) ModifyVehicle(vehicle *model.Vehicle) error {
	return nil
}
