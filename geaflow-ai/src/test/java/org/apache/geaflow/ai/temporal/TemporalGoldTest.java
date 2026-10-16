/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.ai.temporal;

import com.google.gson.Gson;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.geaflow.ai.temporal.integration.IncrementalTemporalIntegrator;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.model.VersionRelationType;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.ai.temporal.query.TemporalQueryService;
import org.apache.geaflow.ai.temporal.semantics.EventLedgerDecision;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.apache.geaflow.ai.temporal.semantics.VersionIdentity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Hand-authored semantic expectations, independent of either replay implementation. */
public class TemporalGoldTest {

    private static final List<String> RESOURCES = Arrays.asList(
        "01-assert-entity-reference", "02-identical-duplicate", "03-event-id-reuse",
        "04-evidence-merge", "05-conflict-coexistence", "06-adjacent-intervals",
        "07-partial-correction", "08-partial-retraction", "09-valid-time-late",
        "10-system-time-disorder", "11-same-timestamp", "12-invalid-target-atomicity");

    private static final Comparator<Instant> END_ORDER =
        Comparator.nullsLast(Comparator.naturalOrder());

    // Check the documented query order independently of the query service.
    private static final Comparator<MemoryFactVersion> QUERY_ORDER =
        Comparator.comparing((MemoryFactVersion v) -> v.getValidTime().getStart())
            .thenComparing(v -> v.getValidTime().getEnd().orElse(null), END_ORDER)
            .thenComparing(v -> v.getTransactionTime().getStart())
            .thenComparing(v -> v.getTransactionTime().getEnd().orElse(null), END_ORDER)
            .thenComparing(MemoryFactVersion::getId);

    @Test
    public void testHandAuthoredGoldStreams() throws Exception {
        for (String name : RESOURCES) {
            try (InputStream input = getClass().getResourceAsStream(
                "/temporal/gold/" + name + ".json")) {
                Assertions.assertNotNull(input, name);
                try (Reader reader = new InputStreamReader(input, StandardCharsets.UTF_8)) {
                    Fixture fixture = new Gson().fromJson(reader, Fixture.class);
                    Assertions.assertEquals(name, fixture.name);
                    new GoldCase(fixture).verify();
                }
            }
        }
    }

    private static final class GoldCase {

        private final Fixture fixture;
        private final EventNormalizer normalizer = new EventNormalizer();
        private final Map<String, EventSpec> events = new HashMap<>();
        private final Map<String, VersionSpec> definitions = new HashMap<>();
        private final Map<String, NormalizedMemoryEvent> normalized = new HashMap<>();
        private final Map<String, String> identities = new HashMap<>();

        private GoldCase(Fixture fixture) {
            this.fixture = fixture;
            Assertions.assertEquals(2, fixture.schemaVersion, fixture.name);
            for (EventSpec event : fixture.events) {
                Assertions.assertNull(events.put(event.alias, event), event.alias);
            }
            for (VersionSpec version : fixture.expected.identities) {
                Assertions.assertNull(definitions.put(version.alias, version), version.alias);
            }
            for (VersionSpec version : fixture.expected.versions) {
                Assertions.assertNull(definitions.put(version.alias, version), version.alias);
            }
        }

        private void verify() {
            IncrementalTemporalIntegrator incremental = new IncrementalTemporalIntegrator();
            FullReplayOracle oracle = new FullReplayOracle();
            List<NormalizedMemoryEvent> committed = new ArrayList<>();
            int start = 0;
            for (BatchSpec batch : fixture.batches) {
                Assertions.assertTrue(batch.endExclusive > start
                    && batch.endExclusive <= fixture.events.size(), fixture.name);
                List<NormalizedMemoryEvent> attempted = new ArrayList<>();
                for (int index = start; index < batch.endExclusive; index++) {
                    attempted.add(normalize(fixture.events.get(index).alias));
                }
                TemporalState before = incremental.stateSnapshot();
                if (batch.expectedFailure != null) {
                    TemporalValidationException error = Assertions.assertThrows(
                        TemporalValidationException.class, () -> incremental.applyBatch(attempted),
                        fixture.name);
                    Assertions.assertEquals(batch.expectedFailure, error.getCode().name(),
                        fixture.name);
                    Assertions.assertEquals(before, incremental.stateSnapshot(),
                        fixture.name + ": rejected batch changed committed state");
                    List<NormalizedMemoryEvent> rejectedReplay = new ArrayList<>(committed);
                    rejectedReplay.addAll(attempted);
                    TemporalValidationException oracleError = Assertions.assertThrows(
                        TemporalValidationException.class,
                        () -> oracle.replayNormalized(rejectedReplay), fixture.name);
                    Assertions.assertEquals(batch.expectedFailure, oracleError.getCode().name(),
                        fixture.name);
                } else {
                    List<EventLedgerDecision> decisions = incremental.applyBatch(attempted);
                    attempted.sort(Comparator.comparing(NormalizedMemoryEvent::getRecordedAt)
                        .thenComparing(NormalizedMemoryEvent::getEventId));
                    Assertions.assertEquals(batch.decisions.size(), decisions.size(), fixture.name);
                    for (int index = 0; index < decisions.size(); index++) {
                        DecisionSpec expected = batch.decisions.get(index);
                        Assertions.assertEquals(normalize(expected.event), attempted.get(index),
                            fixture.name + ": decision event order");
                        Assertions.assertEquals(expected.code, decisions.get(index).name(),
                            fixture.name + ": decision code");
                    }
                    committed.addAll(attempted);
                    Assertions.assertEquals(oracle.replayNormalized(committed),
                        incremental.stateSnapshot(), fixture.name + ": successful batch diff");
                }
                start = batch.endExclusive;
            }
            Assertions.assertEquals(fixture.events.size(), start, fixture.name);
            TemporalState expected = expectedState();
            Assertions.assertEquals(expected, incremental.stateSnapshot(),
                fixture.name + ": hand-authored incremental expectation");
            Assertions.assertEquals(expected, oracle.replayNormalized(committed),
                fixture.name + ": hand-authored Oracle expectation");
            Collections.reverse(committed);
            Assertions.assertEquals(expected, oracle.replayNormalized(committed),
                fixture.name + ": reversed input changes Oracle result");
            verifyQueries(incremental.stateSnapshot());
        }

        private TemporalState expectedState() {
            Map<FactKey, List<MemoryFactVersion>> byKey = new TreeMap<>();
            for (VersionSpec spec : fixture.expected.versions) {
                FactKey key = new FactKey(spec.subject.id, spec.predicate, spec.scope);
                byKey.computeIfAbsent(key, ignored -> new ArrayList<>()).add(version(spec));
            }
            List<VersionRelation> relations = new ArrayList<>();
            for (RelationSpec spec : fixture.expected.relations) {
                relations.add(new VersionRelation(VersionRelationType.valueOf(spec.type),
                    identity(spec.from), identity(spec.to)));
            }
            return new TemporalState(byKey, relations);
        }

        private void verifyQueries(TemporalState state) {
            TemporalQueryService query = new TemporalQueryService();
            List<String> modes = new ArrayList<>();
            for (QuerySpec spec : fixture.expected.queries) {
                modes.add(spec.mode);
                FactKey key = new FactKey(spec.subjectId, spec.predicate, spec.scope);
                List<MemoryFactVersion> actual;
                if ("CURRENT".equals(spec.mode)) {
                    actual = query.current(state, key, time(spec.validAt));
                } else if ("HISTORICAL".equals(spec.mode)) {
                    actual = query.historical(state, key, time(spec.validAt));
                } else if ("AS_OF".equals(spec.mode)) {
                    actual = query.asOf(state, key, time(spec.validAt), time(spec.systemAt));
                } else {
                    throw new AssertionError("Unknown query mode: " + spec.mode);
                }
                List<MemoryFactVersion> expected = new ArrayList<>();
                for (String alias : spec.versions) {
                    Assertions.assertTrue(fixture.expected.versions.stream()
                        .anyMatch(item -> alias.equals(item.alias)),
                        "Query references an unmaterialized identity: " + alias);
                    expected.add(version(definitions.get(alias)));
                }
                expected.sort(QUERY_ORDER);
                Assertions.assertEquals(expected, actual,
                    fixture.name + ": " + spec.mode + " at " + spec.validAt
                        + " / " + spec.systemAt);
            }
            Assertions.assertTrue(modes.containsAll(Arrays.asList("CURRENT", "HISTORICAL", "AS_OF")),
                fixture.name + ": all three query modes are required");
        }

        private NormalizedMemoryEvent normalize(String alias) {
            NormalizedMemoryEvent cached = normalized.get(alias);
            if (cached != null) {
                return cached;
            }
            EventSpec spec = events.get(alias);
            Assertions.assertNotNull(spec, "Unknown input alias: " + alias);
            MemoryEvent event;
            TimeInterval valid = interval(spec.validFrom, spec.validTo);
            List<Evidence> evidence = evidence(spec.evidence);
            if ("ASSERT".equals(spec.operation)) {
                event = MemoryEvent.add(spec.id, fact(spec), valid, time(spec.recordedAt), evidence);
            } else if ("CORRECT".equals(spec.operation)) {
                event = MemoryEvent.correct(spec.id, fact(spec), valid,
                    time(spec.recordedAt), evidence);
            } else if ("RETRACT".equals(spec.operation)) {
                event = MemoryEvent.retract(spec.id, spec.factId, valid,
                    time(spec.recordedAt), evidence);
            } else {
                throw new AssertionError("Unknown operation: " + spec.operation);
            }
            if (spec.target != null) {
                event = event.targeting(definitions.containsKey(spec.target)
                    ? identity(spec.target) : spec.target);
            }
            NormalizedMemoryEvent result = normalizer.normalize(event,
                new FactKey(spec.subject.id, spec.predicate, spec.scope));
            normalized.put(alias, result);
            return result;
        }

        private String identity(String alias) {
            String cached = identities.get(alias);
            if (cached != null) {
                return cached;
            }
            VersionSpec spec = definitions.get(alias);
            Assertions.assertNotNull(spec, "Unknown version alias: " + alias);
            String id = VersionIdentity.create(normalize(spec.event), fact(spec),
                MemoryFactVersionStatus.valueOf(spec.status),
                interval(spec.validFrom, spec.validTo),
                spec.sourceVersion == null ? null : identity(spec.sourceVersion));
            identities.put(alias, id);
            return id;
        }

        private MemoryFactVersion version(VersionSpec spec) {
            return new MemoryFactVersion(identity(spec.alias), fact(spec),
                MemoryFactVersionStatus.valueOf(spec.status),
                interval(spec.validFrom, spec.validTo),
                interval(spec.systemFrom, spec.systemTo), evidence(spec.evidence));
        }
    }

    private static MemoryFact fact(FactSpec spec) {
        MemoryEntity subject = new MemoryEntity(spec.subject.id, spec.subject.label);
        if ("ENTITY_REF".equals(spec.value.kind)) {
            return MemoryFact.relationship(spec.factId, subject, spec.predicate,
                new MemoryEntity(spec.value.value, spec.value.label));
        }
        Assertions.assertEquals("LITERAL", spec.value.kind);
        return MemoryFact.attribute(spec.factId, subject, spec.predicate, spec.value.value);
    }

    private static List<Evidence> evidence(List<EvidenceSpec> specs) {
        List<Evidence> evidence = new ArrayList<>();
        for (EvidenceSpec spec : specs) {
            evidence.add(new Evidence(spec.id, new Source(spec.source.id, spec.source.name),
                spec.content));
        }
        return evidence;
    }

    private static Instant time(String value) {
        return Instant.parse(value);
    }

    private static TimeInterval interval(String start, String end) {
        return new TimeInterval(time(start), end == null ? null : time(end));
    }

    private static final class Fixture {
        private int schemaVersion;
        private String name;
        private List<EventSpec> events;
        private List<BatchSpec> batches;
        private ExpectedSpec expected;
    }

    private static class FactSpec {
        String factId;
        EntitySpec subject;
        String predicate;
        String scope;
        ValueSpec value;
    }

    private static class TimedFactSpec extends FactSpec {
        String alias;
        String validFrom;
        String validTo;
        List<EvidenceSpec> evidence;
    }

    private static final class EventSpec extends TimedFactSpec {
        private String id;
        private String operation;
        private String recordedAt;
        private String target;
    }

    private static final class VersionSpec extends TimedFactSpec {
        private String event;
        private String sourceVersion;
        private String status;
        private String systemFrom;
        private String systemTo;
    }

    private static final class EntitySpec {
        private String id;
        private String label;
    }

    private static final class ValueSpec {
        private String kind;
        private String value;
        private String label;
    }

    private static final class SourceSpec {
        private String id;
        private String name;
    }

    private static final class EvidenceSpec {
        private String id;
        private SourceSpec source;
        private String content;
    }

    private static final class BatchSpec {
        private int endExclusive;
        private String expectedFailure;
        private List<DecisionSpec> decisions;
    }

    private static final class DecisionSpec {
        private String event;
        private String code;
    }

    private static final class ExpectedSpec {
        private List<VersionSpec> identities;
        private List<VersionSpec> versions;
        private List<RelationSpec> relations;
        private List<QuerySpec> queries;
    }

    private static final class RelationSpec {
        private String type;
        private String from;
        private String to;
    }

    private static final class QuerySpec {
        private String mode;
        private String subjectId;
        private String predicate;
        private String scope;
        private String validAt;
        private String systemAt;
        private List<String> versions;
    }
}
