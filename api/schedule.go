package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"gopkg.in/cas.v1"

	"github.com/wtg/shuttletracker/log"
	"github.com/wtg/shuttletracker/model"
)

var (
	lastUpdate time.Time
)

// VehiclesHandler finds all the vehicles in the database.
func (api *API) ScheduleHandler(w http.ResponseWriter, r *http.Request) {
	// Find all schedules in database
	schedule, err := api.db.getSchedule()

	// Handle query errors
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send each schedule to client as JSON
	WriteJSON(w, schedule)
}

// ScheduleCreateHandler adds a new vehicle to the database.
func (api *API) VehiclesCreateHandler(w http.ResponseWriter, r *http.Request) {
	if api.cfg.Authenticate && !cas.IsAuthenticated(r) {
		return
	}

	// Create new scedhule object using request fields
	schedule := model.Schedule{}
	vehicle.currentTime = time.Now()

	// Store new vehicle under vehicles collection
	err = api.db.CreateSchedule(&schedule)
}
