package craft

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var sampleConfiguration Configuration = Configuration{
	Servers: []Server{
		Server{
			Suffrage: Nonvoter,
			ID:       ServerID("id0"),
			Address:  ServerAddress("addr0"),
		},
		Server{
			Suffrage: Voter,
			ID:       ServerID("id1"),
			Address:  ServerAddress("addr1"),
		},
		Server{
			Suffrage: Staging,
			ID:       ServerID("id2"),
			Address:  ServerAddress("addr2"),
		},
	},
}

func TestConfiguration_Configuration_Clone(t *testing.T) {
	cloned := sampleConfiguration.Clone()
	if !reflect.DeepEqual(sampleConfiguration, cloned) {
		t.Fatalf("mismatch %v %v", sampleConfiguration, cloned)
	}
	cloned.Servers[1].ID = "scribble"
	if sampleConfiguration.Servers[1].ID == "scribble" {
		t.Fatalf("cloned configuration shouldn't alias Servers")
	}
}

func TestConfiguration_configurations_Clone(t *testing.T) {
	configurations := configurations{
		committed:      sampleConfiguration,
		committedIndex: 1,
		latest:         sampleConfiguration,
		latestIndex:    2,
	}
	cloned := configurations.Clone()
	if !reflect.DeepEqual(configurations, cloned) {
		t.Fatalf("mismatch %v %v", configurations, cloned)
	}
	cloned.committed.Servers[1].ID = "scribble"
	cloned.latest.Servers[1].ID = "scribble"
	if configurations.committed.Servers[1].ID == "scribble" ||
		configurations.latest.Servers[1].ID == "scribble" {
		t.Fatalf("cloned configuration shouldn't alias Servers")
	}
}

func TestConfiguration_hasVote(t *testing.T) {
	if hasVote(sampleConfiguration, "id0") {
		t.Fatalf("id0 should not have vote")
	}
	if !hasVote(sampleConfiguration, "id1") {
		t.Fatalf("id1 should have vote")
	}
	if hasVote(sampleConfiguration, "id2") {
		t.Fatalf("id2 should not have vote")
	}
	if hasVote(sampleConfiguration, "someotherid") {
		t.Fatalf("someotherid should not have vote")
	}
}

func TestConfiguration_checkConfiguration(t *testing.T) {
	var configuration Configuration
	if checkConfiguration(configuration) == nil {
		t.Fatalf("empty configuration should be error")
	}

	configuration.Servers = append(configuration.Servers, Server{
		Suffrage: Nonvoter,
		ID:       ServerID("id0"),
		Address:  ServerAddress("addr0"),
	})
	if checkConfiguration(configuration) == nil {
		t.Fatalf("lack of voter should be error")
	}

	configuration.Servers = append(configuration.Servers, Server{
		Suffrage: Voter,
		ID:       ServerID("id1"),
		Address:  ServerAddress("addr1"),
	})
	if err := checkConfiguration(configuration); err != nil {
		t.Fatalf("should be OK: %v", err)
	}

	configuration.Servers[1].ID = "id0"
	err := checkConfiguration(configuration)
	if err == nil {
		t.Fatalf("duplicate ID should be error")
	}
	if !strings.Contains(err.Error(), "duplicate ID") {
		t.Fatalf("unexpected error: %v", err)
	}
	configuration.Servers[1].ID = "id1"

	configuration.Servers[1].Address = "addr0"
	err = checkConfiguration(configuration)
	if err == nil {
		t.Fatalf("duplicate address should be error")
	}
	if !strings.Contains(err.Error(), "duplicate address") {
		t.Fatalf("unexpected error: %v", err)
	}
}

var singleServer = Configuration{
	Servers: []Server{
		Server{
			Suffrage: Voter,
			ID:       ServerID("id1"),
			Address:  ServerAddress("addr1x"),
		},
	},
}

var oneOfEach = Configuration{
	Servers: []Server{
		Server{
			Suffrage: Voter,
			ID:       ServerID("id1"),
			Address:  ServerAddress("addr1x"),
		},
		Server{
			Suffrage: Staging,
			ID:       ServerID("id2"),
			Address:  ServerAddress("addr2x"),
		},
		Server{
			Suffrage: Nonvoter,
			ID:       ServerID("id3"),
			Address:  ServerAddress("addr3x"),
		},
	},
}

var voterPair = Configuration{
	Servers: []Server{
		Server{
			Suffrage: Voter,
			ID:       ServerID("id1"),
			Address:  ServerAddress("addr1x"),
		},
		Server{
			Suffrage: Voter,
			ID:       ServerID("id2"),
			Address:  ServerAddress("addr2x"),
		},
	},
}

var nextConfigurationTests = []struct {
	current  Configuration
	command  ConfigurationChangeCommand
	serverID int
	next     string
}{
	// AddStaging: was missing.
	{Configuration{}, AddStaging, 1, "{[{Voter id1 addr1 0}]}"},
	{singleServer, AddStaging, 2, "{[{Voter id1 addr1x 0} {Voter id2 addr2 0}]}"},
	// AddStaging: was Voter.
	{singleServer, AddStaging, 1, "{[{Voter id1 addr1 0}]}"},
	// AddStaging: was Staging.
	{oneOfEach, AddStaging, 2, "{[{Voter id1 addr1x 0} {Voter id2 addr2 0} {Nonvoter id3 addr3x 0}]}"},
	// AddStaging: was Nonvoter.
	{oneOfEach, AddStaging, 3, "{[{Voter id1 addr1x 0} {Staging id2 addr2x 0} {Voter id3 addr3 0}]}"},

	// AddNonvoter: was missing.
	{singleServer, AddNonvoter, 2, "{[{Voter id1 addr1x 0} {Nonvoter id2 addr2 0}]}"},
	// AddNonvoter: was Voter.
	{singleServer, AddNonvoter, 1, "{[{Voter id1 addr1 0}]}"},
	// AddNonvoter: was Staging.
	{oneOfEach, AddNonvoter, 2, "{[{Voter id1 addr1x 0} {Staging id2 addr2 0} {Nonvoter id3 addr3x 0}]}"},
	// AddNonvoter: was Nonvoter.
	{oneOfEach, AddNonvoter, 3, "{[{Voter id1 addr1x 0} {Staging id2 addr2x 0} {Nonvoter id3 addr3 0}]}"},

	// DemoteVoter: was missing.
	{singleServer, DemoteVoter, 2, "{[{Voter id1 addr1x 0}]}"},
	// DemoteVoter: was Voter.
	{voterPair, DemoteVoter, 2, "{[{Voter id1 addr1x 0} {Nonvoter id2 addr2x 0}]}"},
	// DemoteVoter: was Staging.
	{oneOfEach, DemoteVoter, 2, "{[{Voter id1 addr1x 0} {Nonvoter id2 addr2x 0} {Nonvoter id3 addr3x 0}]}"},
	// DemoteVoter: was Nonvoter.
	{oneOfEach, DemoteVoter, 3, "{[{Voter id1 addr1x 0} {Staging id2 addr2x 0} {Nonvoter id3 addr3x 0}]}"},

	// RemoveServer: was missing.
	{singleServer, RemoveServer, 2, "{[{Voter id1 addr1x 0}]}"},
	// RemoveServer: was Voter.
	{voterPair, RemoveServer, 2, "{[{Voter id1 addr1x 0}]}"},
	// RemoveServer: was Staging.
	{oneOfEach, RemoveServer, 2, "{[{Voter id1 addr1x 0} {Nonvoter id3 addr3x 0}]}"},
	// RemoveServer: was Nonvoter.
	{oneOfEach, RemoveServer, 3, "{[{Voter id1 addr1x 0} {Staging id2 addr2x 0}]}"},

	// Promote: was missing.
	{singleServer, Promote, 2, "{[{Voter id1 addr1x 0}]}"},
	// Promote: was Voter.
	{singleServer, Promote, 1, "{[{Voter id1 addr1x 0}]}"},
	// Promote: was Staging.
	{oneOfEach, Promote, 2, "{[{Voter id1 addr1x 0} {Voter id2 addr2x 0} {Nonvoter id3 addr3x 0}]}"},
	// Promote: was Nonvoter.
	{oneOfEach, Promote, 3, "{[{Voter id1 addr1x 0} {Staging id2 addr2x 0} {Nonvoter id3 addr3x 0}]}"},
}

func TestConfiguration_nextConfiguration_table(t *testing.T) {
	for i, tt := range nextConfigurationTests {
		req := configurationChangeRequest{
			command:       tt.command,
			serverID:      ServerID(fmt.Sprintf("id%d", tt.serverID)),
			serverAddress: ServerAddress(fmt.Sprintf("addr%d", tt.serverID)),
		}
		next, err := nextConfiguration(tt.current, 1, req)
		if err != nil {
			t.Errorf("nextConfiguration %d should have succeeded, got %v", i, err)
			continue
		}
		if fmt.Sprintf("%v", next) != tt.next {
			t.Errorf("nextConfiguration %d returned %v, expected %s", i, next, tt.next)
			continue
		}
	}
}

func TestConfiguration_nextConfiguration_prevIndex(t *testing.T) {
	// Stale prevIndex.
	req := configurationChangeRequest{
		command:       AddStaging,
		serverID:      ServerID("id1"),
		serverAddress: ServerAddress("addr1"),
		prevIndex:     1,
	}
	_, err := nextConfiguration(singleServer, 2, req)
	if err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("nextConfiguration should have failed due to intervening configuration change")
	}

	// Current prevIndex.
	req = configurationChangeRequest{
		command:       AddStaging,
		serverID:      ServerID("id2"),
		serverAddress: ServerAddress("addr2"),
		prevIndex:     2,
	}
	_, err = nextConfiguration(singleServer, 2, req)
	if err != nil {
		t.Fatalf("nextConfiguration should have succeeded, got %v", err)
	}

	// Zero prevIndex.
	req = configurationChangeRequest{
		command:       AddStaging,
		serverID:      ServerID("id3"),
		serverAddress: ServerAddress("addr3"),
		prevIndex:     0,
	}
	_, err = nextConfiguration(singleServer, 2, req)
	if err != nil {
		t.Fatalf("nextConfiguration should have succeeded, got %v", err)
	}
}

func TestConfiguration_nextConfiguration_checkConfiguration(t *testing.T) {
	req := configurationChangeRequest{
		command:       AddNonvoter,
		serverID:      ServerID("id1"),
		serverAddress: ServerAddress("addr1"),
	}
	_, err := nextConfiguration(Configuration{}, 1, req)
	if err == nil || !strings.Contains(err.Error(), "at least one voter") {
		t.Fatalf("nextConfiguration should have failed for not having a voter")
	}
}

func TestConfiguration_encodeDecodePeers(t *testing.T) {
	// Set up configuration.
	var configuration Configuration
	for i := 0; i < 3; i++ {
		address := NewInmemAddr()
		configuration.Servers = append(configuration.Servers, Server{
			Suffrage: Voter,
			ID:       ServerID(address),
			Address:  ServerAddress(address),
		})
	}

	// Encode into the old format.
	_, trans := NewInmemTransport("")
	buf := encodePeers(configuration, trans)

	// Decode from old format, as if reading an old log entry.
	decoded := decodePeers(buf, trans)
	if !reflect.DeepEqual(configuration, decoded) {
		t.Fatalf("mismatch %v %v", configuration, decoded)
	}
}

func TestConfiguration_encodeDecodeConfiguration(t *testing.T) {
	decoded := decodeConfiguration(encodeConfiguration(sampleConfiguration))
	if !reflect.DeepEqual(sampleConfiguration, decoded) {
		t.Fatalf("mismatch %v %v", sampleConfiguration, decoded)
	}
}
