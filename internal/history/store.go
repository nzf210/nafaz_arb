package history

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

type Episode struct {
	OpportunityID string `json:"opportunity_id"`
	OpportunityKey string `json:"opportunity_key"`
	BaseAsset string `json:"base_asset"`
	QuoteAsset string `json:"quote_asset"`
	LifecycleStatus string `json:"lifecycle_status"`
	FirstSeenAt time.Time `json:"first_seen_at"`
	LastSeenAt time.Time `json:"last_seen_at"`
	EndedAt *time.Time `json:"ended_at,omitempty"`
	ObservedDurationSeconds int64 `json:"observed_duration_seconds"`
	ScanObservationCount int `json:"scan_observation_count"`
	MissedSuccessfulScanCount int `json:"missed_successful_scan_count"`
	EndReason string `json:"end_reason,omitempty"`
}
type Metrics struct {
	NewOpportunities int `json:"new_opportunities"`
	ActiveOpportunities int `json:"active_opportunities"`
	EndedOpportunities int `json:"ended_opportunities"`
	TotalScanObservations int `json:"total_scan_observations"`
	DurationSampleCount int `json:"duration_sample_count"`
	AverageObservedDurationSeconds float64 `json:"average_observed_duration_seconds"`
	MedianObservedDurationSeconds float64 `json:"median_observed_duration_seconds"`
	MaxObservedDurationSeconds int64 `json:"max_observed_duration_seconds"`
}
type Store struct { mu sync.Mutex; path string; episodes map[string]Episode }
func New(path string) (*Store,error) {
	s:=&Store{path:path,episodes:map[string]Episode{}}
	b,err:=os.ReadFile(path); if err==nil { if e:=json.Unmarshal(b,&s.episodes); e!=nil { return nil,fmt.Errorf("decode history: %w",e) } } else if !os.IsNotExist(err) { return nil,err }
	return s,nil
}
func (s *Store) Observe(now time.Time, candidates []domain.Opportunity, complete bool, maxMisses int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if maxMisses < 1 {
		maxMisses = 1
	}
	seen := map[string]bool{}
	for _, c := range candidates {
		if !c.TradingEligible {
			continue
		}
		seen[c.OpportunityKey] = true
		s.recordCandidate(c, now)
	}
	if complete {
		s.expireMissed(seen, now, maxMisses)
	}
	return s.persistLocked()
}

func (s *Store) recordCandidate(c domain.Opportunity, now time.Time) {
	activeID := s.findActiveEpisodeID(c.OpportunityKey)
	if activeID == "" {
		activeID = newID()
		s.episodes[activeID] = Episode{
			OpportunityID:        activeID,
			OpportunityKey:       c.OpportunityKey,
			BaseAsset:            c.BaseAsset,
			QuoteAsset:           c.QuoteAsset,
			LifecycleStatus:      "ACTIVE",
			FirstSeenAt:          now,
			LastSeenAt:           now,
			ScanObservationCount: 1,
		}
		return
	}
	e := s.episodes[activeID]
	e.LastSeenAt = now
	e.ScanObservationCount++
	e.MissedSuccessfulScanCount = 0
	s.episodes[activeID] = e
}

func (s *Store) findActiveEpisodeID(key string) string {
	for id, e := range s.episodes {
		if e.OpportunityKey == key && e.LifecycleStatus == "ACTIVE" {
			return id
		}
	}
	return ""
}

func (s *Store) expireMissed(seen map[string]bool, now time.Time, maxMisses int) {
	for id, e := range s.episodes {
		if e.LifecycleStatus != "ACTIVE" || seen[e.OpportunityKey] {
			continue
		}
		e.MissedSuccessfulScanCount++
		if e.MissedSuccessfulScanCount >= maxMisses {
			ended := now
			e.LifecycleStatus = "ENDED"
			e.EndedAt = &ended
			e.EndReason = "MISSED_SUCCESSFUL_SCANS"
			e.ObservedDurationSeconds = int64(e.LastSeenAt.Sub(e.FirstSeenAt).Seconds())
		}
		s.episodes[id] = e
	}
}
func (s *Store) List(from,to time.Time,status string) []Episode {
	s.mu.Lock(); defer s.mu.Unlock(); out:=[]Episode{}
	for _,e:=range s.episodes { if !from.IsZero() && e.FirstSeenAt.Before(from) { continue }; if !to.IsZero() && e.FirstSeenAt.After(to) { continue }; if status!="" && e.LifecycleStatus!=status { continue }; out=append(out,e) }
	sort.Slice(out,func(i,j int)bool{return out[i].FirstSeenAt.After(out[j].FirstSeenAt)}); return out
}
func (s *Store) GetMetrics(from,to,now time.Time) Metrics {
	items:=s.List(from,to,""); m:=Metrics{}; var durations []float64; sum:=0.0
	for _,e:=range items {
		m.NewOpportunities++
		m.TotalScanObservations+=e.ScanObservationCount
		if e.LifecycleStatus=="ACTIVE" { m.ActiveOpportunities++ }
		if e.LifecycleStatus=="ENDED" && e.EndedAt!=nil && (from.IsZero()||!e.EndedAt.Before(from)) && (to.IsZero()||!e.EndedAt.After(to)) {
			m.EndedOpportunities++; d:=float64(e.ObservedDurationSeconds); durations=append(durations,d); sum+=d; if e.ObservedDurationSeconds>m.MaxObservedDurationSeconds {m.MaxObservedDurationSeconds=e.ObservedDurationSeconds}
		}
	}
	m.DurationSampleCount=len(durations); if len(durations)>0 { m.AverageObservedDurationSeconds=sum/float64(len(durations)); sort.Float64s(durations); mid:=len(durations)/2; if len(durations)%2==1 {m.MedianObservedDurationSeconds=durations[mid]} else {m.MedianObservedDurationSeconds=(durations[mid-1]+durations[mid])/2} }
	_ = now; return m
}
func (s *Store) persistLocked() error {
	if s.path=="" { return nil }; if err:=os.MkdirAll(filepath.Dir(s.path),0755); err!=nil{return err}
	b,err:=json.MarshalIndent(s.episodes,"","  "); if err!=nil{return err}; tmp:=s.path+".tmp"
	if err=os.WriteFile(tmp,b,0600); err!=nil{return err}; return os.Rename(tmp,s.path)
}
func newID() string { return fmt.Sprintf("opp-%d-%d", time.Now().UnixNano(), os.Getpid()) }
