//go:build integration
// +build integration

package gocql

import (
	"context"
	"fmt"
	"testing"
)

// Check if TokenAwareHostPolicy works correctly when using tablets
func TestTablets(t *testing.T) {
	if !isTabletsSupported() {
		t.Skip("Tablets are not supported by this server")
	}
	cluster := createCluster()

	fallback := RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(fallback)

	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if err := createTable(session, fmt.Sprintf(`CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck));
	`, "test_tablets")); err != nil {
		t.Fatalf("unable to create table: %v", err)
	}

	hosts := session.hostSource.getHostsList()

	hostAddresses := []string{}
	for _, host := range hosts {
		hostAddresses = append(hostAddresses, host.connectAddress.String())
	}

	ctx := context.Background()

	i := 0
	for i < 50 {
		i = i + 1
		err := session.Query(`INSERT INTO test_tablets (pk, ck, v) VALUES (?, ?, ?);`, i, i%5, i%2).WithContext(ctx).Exec()
		if err != nil {
			t.Fatal(err)
		}
	}

	trace := NewTracer(session)
	i = 0
	for i < 50 {
		i = i + 1

		var pk int
		var ck int
		var v int

		err := session.Query(`SELECT pk, ck, v FROM test_tablets WHERE pk = ?;`, i).WithContext(ctx).Consistency(One).Trace(trace).Scan(&pk, &ck, &v)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, traceID := range trace.AllTraceIDs() {
		var (
			isReady bool
			err     error
		)
		for !isReady {
			isReady, err = trace.IsReady(traceID)
			if err != nil {
				t.Fatal("Error: ", err)
			}
		}

		hosts := []string{}
		activities, err := trace.GetActivities(traceID)
		if err != nil {
			t.Fatal(err)
		}
		coordinator, _, err := trace.GetCoordinatorTime(traceID)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range activities {
			hosts = append(hosts, entry.Source)
		}
		hosts = append(hosts, coordinator)

		// find duplicates to check how many hosts are used
		allHosts := make(map[string]bool)
		hostList := []string{}
		for _, item := range hosts {
			if !allHosts[item] {
				allHosts[item] = true
				hostList = append(hostList, item)
			}
		}

		if len(hostList) != 1 {
			t.Fatalf("trace should show only one host but it showed %d hosts, hosts: %s", len(hostList), hostAddresses)
		}
	}
}

// Check if shard awareness works correctly when using tablets
func TestTabletsShardAwareness(t *testing.T) {
	if !isTabletsSupported() {
		t.Skip("Tablets are not supported by this server")
	}
	cluster := createCluster()
	_ = createSessionFromCluster(cluster, t)

	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())

	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if err := createTable(session, fmt.Sprintf(`CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck));
	`, "test_tablets_shard_awarness")); err != nil {
		t.Fatalf("unable to create table: %v", err)
	}

	ctx := context.Background()

	for i := 0; i < 50; i++ {
		err := session.Query(`INSERT INTO test_tablets_shard_awarness (pk, ck, v) VALUES (?, ?, ?);`, i, i%5, i%2).WithContext(ctx).Exec()
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 50; i++ {
		// After 2025.2 tablet migration can happen in the middle of tests
		// When it happens queries can hit wrong tablet despite tablet been just learned
		// Here we go for second attempt when it happens
		// Assumption is that having second tablet migration right away is impossible
		for attempt := 1; true; attempt++ {
			trace := NewTracer(session)
			var pk int
			var ck int
			var v int
			query := session.Query(`SELECT pk, ck, v FROM test_tablets_shard_awarness WHERE pk = ?;`, i).WithContext(ctx).Consistency(One).Trace(trace)
			err := query.Scan(&pk, &ck, &v)
			if err != nil {
				t.Fatal(err)
			}

			shardsParticipated := getNumberOfShardsParticipated(t, trace)
			if len(shardsParticipated) == 1 {
				break
			}
			if attempt >= 2 {
				t.Fatalf("trace should show only one shard but it showed %d shards, shards: %s", len(shardsParticipated), shardsParticipated)
			}
		}
	}
}

func getNumberOfShardsParticipated(t *testing.T, trace *TracerEnhanced) []string {
	allShards := make(map[string]bool)
	shardList := []string{}

	for _, traceID := range trace.AllTraceIDs() {
		var (
			isReady bool
			err     error
		)
		for !isReady {
			isReady, err = trace.IsReady(traceID)
			if err != nil {
				t.Fatal("Error: ", err)
			}
		}

		activities, err := trace.GetActivities(traceID)
		if err != nil {
			t.Fatal(err)
		}

		// find duplicates to check how many shards are used
		for _, item := range activities {
			if !allShards[item.Thread] {
				allShards[item.Thread] = true
				shardList = append(shardList, item.Thread)
			}
		}
	}
	return shardList
}
