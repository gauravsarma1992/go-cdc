package mongoreplay

import (
	"context"
	"errors"
	"log"
	"time"
)

type (
	StageTypeT uint8
	StateTypeT uint8

	StageTracker struct {
		Ctx       context.Context
		CurrStage StageTypeT
		Stages    map[StageTypeT]*Stage
		stageMap  map[StageTypeT]StageFunction

		SrcCollection *OplogCollection
		DstCollection *OplogCollection
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

	NoopStageExecutor struct{}
)

var (
	// Stages
	InitStage                StageTypeT = 0
	PreparingCollectionStage StageTypeT = 1
	DumpingCollectionStage   StageTypeT = 2
	TailingOplogStage        StageTypeT = 3

	// States
	PendingState StateTypeT = 0
	SuccessState StateTypeT = 1
	FailedState  StateTypeT = 2
)

func (noopStageExecutor *NoopStageExecutor) Run(args ...interface{}) (err error) {
	return
}

func NewNoopStageExecutor(ctx context.Context, srcCollection *OplogCollection, dstCollection *OplogCollection) (stageExecutor StageExecutor, err error) {
	noopStageExecutor := &NoopStageExecutor{}
	stageExecutor = noopStageExecutor
	return
}

func NewStageTracker(ctx context.Context, srcCollection *OplogCollection, dstCollection *OplogCollection) (stageTracker *StageTracker, err error) {
	stageTracker = &StageTracker{
		Ctx:           ctx,
		SrcCollection: srcCollection,
		DstCollection: dstCollection,
		stageMap:      make(map[StageTypeT]StageFunction),
		Stages:        make(map[StageTypeT]*Stage),
	}
	if err = stageTracker.prepareStageMap(); err != nil {
		return
	}
	return
}

func (stageTracker *StageTracker) prepareStageMap() (err error) {
	stageTracker.stageMap[InitStage] = NewNoopStageExecutor
	stageTracker.stageMap[PreparingCollectionStage] = NewNoopStageExecutor
	stageTracker.stageMap[DumpingCollectionStage] = NewDumper
	stageTracker.stageMap[TailingOplogStage] = NewTailerManager
	return
}

func (stageTracker *StageTracker) RunStage(args ...interface{}) (err error) {
	var (
		stageFunction StageFunction
		stageExecutor StageExecutor
	)
	log.Println("[StageTracker] Running stage", stageTracker.CurrStage+1)
	stageFunction = stageTracker.stageMap[stageTracker.CurrStage]

	stageTracker.Stages[stageTracker.CurrStage] = &Stage{
		StartTime: time.Now(),
		StageType: stageTracker.CurrStage,
		Status:    PendingState,
		Metadata:  make(map[string]interface{}),
	}
	if stageExecutor, err = stageFunction(stageTracker.Ctx, stageTracker.SrcCollection, stageTracker.DstCollection); err != nil {
		return
	}
	if err = stageExecutor.Run(args); err != nil {
		return
	}
	if err = stageTracker.Next(args); err != nil {
		return
	}
	if stageTracker.RunStage(args); err != nil {
		return
	}
	return
}

func (stageTracker *StageTracker) Next(args ...interface{}) (err error) {
	// Updating the attributes of the current stage
	stageTracker.Stages[stageTracker.CurrStage].StopTime = time.Now()
	stageTracker.Stages[stageTracker.CurrStage].LastHeartbeatAt = time.Now()
	stageTracker.Stages[stageTracker.CurrStage].Status = SuccessState

	// Creating the next stage
	if int(stageTracker.CurrStage) == len(stageTracker.stageMap)-1 {
		err = errors.New("No stages left to run")
		return
	}
	stageTracker.CurrStage += 1
	if err = stageTracker.RunStage(args); err != nil {
		return
	}
	return
}

func (stageTracker *StageTracker) Run() (err error) {
	log.Println("[StageTracker] Starting stage tracker")
	stageTracker.CurrStage = InitStage
	if err = stageTracker.RunStage(); err != nil {
		return
	}
	return
}
