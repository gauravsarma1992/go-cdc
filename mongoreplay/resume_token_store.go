package mongoreplay

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	LastUpdatedResumeFile = "/tmp/last-updated-resume-token"
)

type (
	ResumeTokenStore struct {
		Timestamp primitive.Timestamp `json:"timestamp"`
	}
)

func (resumeTokenStore *ResumeTokenStore) getFileName() (fileName string) {
	fileName = fmt.Sprintf("%s", LastUpdatedResumeFile)
	return
}

func (resumeToken *ResumeTokenStore) Copy() (copied *ResumeTokenStore) {
	copied = &ResumeTokenStore{
		Timestamp: resumeToken.Timestamp,
	}
	return
}

func (resumeTokenStore *ResumeTokenStore) Store() (err error) {
	var (
		resumeB []byte
	)
	if resumeB, err = json.Marshal(resumeTokenStore); err != nil {
		return
	}
	if err = ioutil.WriteFile(resumeTokenStore.getFileName(), resumeB, 0755); err != nil {
		return
	}
	return
}

func (resumeTokenStore ResumeTokenStore) Fetch() (resumeToken *ResumeTokenStore, err error) {
	var (
		resumeB []byte
	)
	resumeToken = &ResumeTokenStore{}
	if resumeB, err = ioutil.ReadFile(resumeTokenStore.getFileName()); err != nil {
		return
	}
	if err = json.Unmarshal(resumeB, resumeToken); err != nil {
		return
	}
	return
}
