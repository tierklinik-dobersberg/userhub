package rosterapi

import "github.com/gin-gonic/gin"

// Setup registers all API routes for the roster.
func Setup(grp gin.IRouter) {
	// GET /api/dutyroster/v1/:year/:month
	GetRosterEndpoint(grp)
}
