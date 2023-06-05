package mongoreplay

import (
	"context"
	"time"
)

type (
	StageTypeT uint8
	StateTypeT uint8

	StageTracker struct {
		Ctx    context.Context
		Stages []Stage
	}
	Stage struct {
		StartTime       time.Time `json:"start_time"`
		StopTime        time.Time `json:"stop_time"`
		LastHeartbeatAt time.Time `json:"last_heartbeat_at"`

		StageType StageTypeT `json:"stage_type"`
		Status    StateTypeT `json:"status"`

		Metadata map[string]interface{} `json:"metadata"`
	}
)

var (
	// Stages
	InitStage                StageTypeT = 0
	PreparingCollectionStage StageTypeT = 1
	DumpingCollectionStage   StageTypeT = 2
	TailingOplogStage        StageTypeT = 3

	// States
	SuccessState StateTypeT = 0
	FailedState  StateTypeT = 1
)

func NewStageTracker(ctx context.Context) (stageTracker *StageTracker, err error) {
	stageTracker = &StageTracker{
		Ctx: ctx,
	}
	return
}

func (stageTracker *StageTracker) Track() (err error) {
	return
}
