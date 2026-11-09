import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type { ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as ProjectFeed from "./ProjectFeed.ts";

/** Project refs the test resolver does not know. */
export const MISSING_PROJECT = "missing-project";

/**
 * The feed over a bare event sink and projection store, as tests and the benchmark need it. The
 * sink and the feed share one application event store, as they do in the server. Any project id
 * resolves, except MISSING_PROJECT.
 */
export const layerProjectFeedOn = <E, R>(database: Layer.Layer<SqlClient.SqlClient, E, R>) => {
  const applicationEvents = OrchestrationEventStore.layer.pipe(Layer.provide(database));
  const stores = Layer.mergeAll(
    applicationEvents,
    EventStore.layerFromOrchestrationEventStore.pipe(Layer.provide(applicationEvents)),
    ProjectionStore.layer.pipe(Layer.provide(database)),
  );
  const sink = EventSink.layer.pipe(Layer.provide(Layer.mergeAll(stores, database)));
  const feed = Layer.effect(
    ProjectFeed.ProjectFeed,
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const events = yield* OrchestrationEventStore.OrchestrationEventStore;
      return yield* ProjectFeed.make(
        ProjectFeed.sourceFrom({
          resolveProject: (ref) =>
            Effect.succeed(
              ref === MISSING_PROJECT
                ? null
                : { id: ref as ProjectId, title: `Project ${ref}`, workspaceRoot: `/work/${ref}` },
            ),
          getShellSnapshot: projections.getShellSnapshot,
          events,
        }),
      );
    }),
  );
  return Layer.mergeAll(sink, stores, IdAllocator.layer, database).pipe(
    Layer.provideMerge(feed.pipe(Layer.provide(Layer.mergeAll(sink, stores, database)))),
  );
};
