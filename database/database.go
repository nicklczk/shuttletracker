package database

import (
	"errors"
	"time"

	"github.com/wtg/shuttletracker/model"
)

// Database is an interface that can be implemented by a database backend.
type Database interface {
	// Routes
	CreateRoute(route *model.Route) error
	DeleteRoute(routeID string) error
	GetRoute(routeID string) (model.Route, error)
	GetRoutes() ([]model.Route, error)
	ModifyRoute(route *model.Route) error

	// Stops
	CreateStop(stop *model.Stop) error
	DeleteStop(stopID string) error
	GetStops() ([]model.Stop, error)
	// GetStopsForRoute(routeID string) ([]model.Stop, error)
	// ModifyStop(stop *model.Stop) error
	AddStopToRoute(stopID string, routeID string) error

	// Vehicles
	CreateVehicle(vehicle *model.Vehicle) error
	DeleteVehicle(vehicleID int) error
	GetVehicle(vehicleID int) (model.Vehicle, error)
	GetVehicles() ([]model.Vehicle, error)
	GetVehicleByITrakID(itrakID int) (model.Vehicle, error)
	GetEnabledVehicles() ([]model.Vehicle, error)
	ModifyVehicle(vehicle *model.Vehicle) error

	// Updates
	CreateUpdate(update *model.Update) error
	DeleteUpdatesBefore(before time.Time) (int64, error)
	// GetUpdatesSince(since time.Time) ([]model.VehicleUpdate, error)
	GetUpdatesForVehicleSince(vehicleID int, since time.Time) ([]model.Update, error)
	GetLastUpdateForVehicle(vehicleID int) (model.Update, error)

	// Users
	GetUsers() ([]model.User, error)
}

var (
	ErrVehicleNotFound = errors.New("Vehicle not found.")
	ErrUpdateNotFound  = errors.New("Update not found.")
)
