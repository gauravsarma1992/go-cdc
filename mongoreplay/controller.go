package mongoreplay

import (
	"context"
)

type (
	Controller struct {
		Ctx context.Context

		SourceCollection *OplogCollection
		DestCollection   *OplogCollection

		StageTracker *StageTracker

		trackerCloseCh chan bool
	}
)

func NewController(ctx context.Context, sourceCollection *OplogCollection, destCollection *OplogCollection) (controller *Controller, err error) {
	controller = &Controller{
		Ctx:              ctx,
		SourceCollection: sourceCollection,
		DestCollection:   destCollection,
		trackerCloseCh:   make(chan bool),
	}
	if controller.StageTracker, err = NewStageTracker(ctx, sourceCollection, destCollection); err != nil {
		return
	}
	return
}

func (controller *Controller) Run() (err error) {
	if err = controller.StageTracker.Run(); err != nil {
		return
	}
	return
}
