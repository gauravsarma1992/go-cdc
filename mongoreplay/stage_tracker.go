package mongoreplay

import (
	"context"
	"time"
)

type (
	StageTypeT uint8
	StateTypeT uint8

	StageTracker struct {
		Ctx      context.Context
		Stages   []Stage
		stageMap map[StageTypeT]StageFunction
	}
	Stage struct {
		StartTime       time.Time `json:"start_time"`
		StopTime        time.Time `json:"stop_time"`
		LastHeartbeatAt time.Time `json:"last_heartbeat_at"`

		StageType StageTypeT `json:"stage_type"`
		Status    StateTypeT `json:"status"`

		Metadata map[string]interface{} `json:"metadata"`
	}
	StageExecutor interface {
		Run(...interface{}) error
	}
	StageFunction func(context.Context, *OplogCollection, *OplogCollection) (StageExecutor, error)
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
		Ctx:      ctx,
		stageMap: make(map[StageTypeT]StageFunction),
	}
	return
}

func (stageTracker *StageTracker) prepareStageMap() (err error) {
	stageTracker.stageMap[DumpingCollectionStage] = NewDumper
	stageTracker.stageMap[TailingOplogStage] = NewOplogWatcher
	return
}

func (stageTracker *StageTracker) Track() (err error) {
	return
}
