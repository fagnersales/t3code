import { describe, expect, it } from "vite-plus/test";

import {
  buildThreadActionMenuItems,
  buildWorkspaceMoveMenuItem,
  threadActionRequiresOperate,
  type ThreadActionMenuState,
  workspaceOfMoveMenuId,
} from "./threadActionMenu.logic";

const baseState: ThreadActionMenuState = {
  canOperate: true,
  branch: null,
  projectFilter: null,
  isPinned: false,
  isSettled: false,
  autoSettleEnabled: true,
  isSnoozed: false,
  canSnoozeNow: true,
  isRegeneratingTitle: false,
  isRunning: false,
  supports: {
    settlement: true,
    autoSettleOptOut: true,
    snooze: true,
    pinning: true,
    titleRegeneration: true,
  },
  snoozePresets: [
    { id: "hour", label: "In 1 hour", whenLabel: "3:00 PM", snoozedUntil: "2026-08-07T15:00:00Z" },
  ],
};

describe("buildThreadActionMenuItems", () => {
  it.each([false, true])(
    "disables both lifecycle directions without permission (reversed: %s)",
    (reversed) => {
      const items = buildThreadActionMenuItems({
        ...baseState,
        canOperate: false,
        isPinned: reversed,
        isSettled: reversed,
        isSnoozed: reversed,
      });
      const expected = reversed
        ? [
            "unpin",
            "unsettle",
            "unsnooze",
            "rename",
            "regenerate-title",
            "auto-settle",
            "archive",
            "delete",
          ]
        : [
            "pin",
            "settle",
            "snooze",
            "rename",
            "regenerate-title",
            "auto-settle",
            "archive",
            "delete",
          ];
      expect(items.filter((item) => item.disabled).map((item) => item.id)).toEqual(expected);
      expect(
        items.find((item) => item.id === "snooze")?.children?.every((child) => child.disabled) ??
          true,
      ).toBe(true);
    },
  );

  it("disables archive and turning into a project while the thread is running", () => {
    const items = buildThreadActionMenuItems({
      ...baseState,
      isRunning: true,
      canTurnIntoProject: true,
    });
    expect(items.find((item) => item.id === "archive")?.disabled).toBe(true);
    expect(items.find((item) => item.id === "turn-into-project")?.disabled).toBe(true);
  });
});

describe("workspace move items", () => {
  it("names the one target and maps the item back to it", () => {
    const toHq = buildWorkspaceMoveMenuItem({
      targets: ["Headquarters"],
      count: null,
      disabled: false,
    });
    expect(toHq).toMatchObject({ label: "Move to Headquarters" });
    expect(workspaceOfMoveMenuId(toHq!.id)).toBe("Headquarters");
    const back = buildWorkspaceMoveMenuItem({ targets: [null], count: 3, disabled: false });
    expect(back).toMatchObject({ id: "move-to-threads", label: "Move to Threads (3)" });
    expect(workspaceOfMoveMenuId(back!.id)).toBeNull();
  });

  it("nests several targets and offers nothing without one", () => {
    const item = buildWorkspaceMoveMenuItem({
      targets: [null, "Headquarters"],
      count: 2,
      disabled: true,
    });
    expect(item?.label).toBe("Move to workspace (2)");
    expect(workspaceOfMoveMenuId(item!.id)).toBeUndefined();
    expect(item?.children?.map((child) => child.label)).toEqual(["Threads", "Headquarters"]);
    expect(buildWorkspaceMoveMenuItem({ targets: [], count: null, disabled: false })).toBeNull();
  });

  it("leaves the settings scope check to the caller", () => {
    expect(threadActionRequiresOperate("move-to-threads")).toBe(false);
    expect(threadActionRequiresOperate("move-to-workspace:Headquarters")).toBe(false);
  });
});
