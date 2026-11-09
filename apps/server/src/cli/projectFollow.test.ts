import { assert, it } from "vite-plus/test";

import { parseProjectFollowQuery } from "../projectFeed/http.ts";
import {
  type FollowedProject,
  parseProjectArguments,
  projectFeedUrl,
  trackProjectEvent,
} from "./projectFollow.ts";

it("project arguments take a shared --after, an own @position, and keep @ inside paths", () => {
  assert.deepEqual(parseProjectArguments(["project-a", "/w/b@42", "/w/me@home", "project-a"], 7), [
    { ref: "project-a", projectId: undefined, position: 7 },
    { ref: "/w/b", projectId: undefined, position: 42 },
    { ref: "/w/me@home", projectId: undefined, position: 7 },
  ]);
  assert.typeOf(parseProjectArguments([], undefined), "string");
  assert.typeOf(parseProjectArguments(["@5"], undefined), "string");
});

it("the feed route reads the same project@position form, after and archived", () => {
  assert.deepEqual(
    parseProjectFollowQuery(new URLSearchParams("project=p1&project=/w/b@42&after=7&archived=1")),
    [
      { ref: "p1", after: 7, includeArchived: true },
      { ref: "/w/b", after: 42, includeArchived: true },
    ],
  );
  assert.typeOf(parseProjectFollowQuery(new URLSearchParams("after=1")), "string");
  assert.typeOf(parseProjectFollowQuery(new URLSearchParams("project=a&after=-1")), "string");
  assert.typeOf(parseProjectFollowQuery(new URLSearchParams("project=a&archived=yes")), "string");
});

it("a reconnect names each project by id at the last position it printed", () => {
  const projects: Array<FollowedProject> = [
    { ref: "/w/blox", projectId: undefined, position: undefined },
    { ref: "/w/gone", projectId: undefined, position: undefined },
  ];
  trackProjectEvent(projects, {
    v: 1,
    type: "snapshot",
    projectId: "p-blox",
    position: 100,
    ref: "/w/blox",
    threads: 0,
  });
  trackProjectEvent(projects, {
    v: 1,
    type: "ready",
    projectId: "p-blox",
    position: 100,
    ref: "/w/blox",
    title: "Blox",
    workspaceRoot: "/w/blox",
    threads: 0,
    resumed: false,
  });
  trackProjectEvent(projects, {
    v: 1,
    type: "removed",
    projectId: "p-blox",
    position: 130,
    threadId: "t1",
    reason: "deleted",
  });
  trackProjectEvent(projects, {
    v: 1,
    type: "error",
    ref: "/w/gone",
    projectId: null,
    position: null,
    message: "No project",
  });
  assert.deepEqual(projects, [{ ref: "/w/blox", projectId: "p-blox", position: 130 }]);
  const url = projectFeedUrl("http://127.0.0.1:3773", projects, false);
  assert.equal(url.pathname, "/api/project-feed/v1");
  assert.deepEqual(url.searchParams.getAll("project"), ["p-blox@130"]);
});
