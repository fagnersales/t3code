import { Building2Icon, LayersIcon } from "lucide-react";

import { cn } from "~/lib/utils";
import type { SidebarWorkspace } from "../Sidebar.logic";
import { Menu, MenuPopup, MenuRadioGroup, MenuRadioItem, MenuTrigger } from "../ui/menu";
import { SidebarHeaderIconButton } from "./SidebarThreadHeader";

const THREADS_LABEL = "Threads";
// Base UI radio values cannot be null.
const THREADS_VALUE = "";

const labelOf = (workspace: SidebarWorkspace) => workspace ?? THREADS_LABEL;

function WaitingBadge({ count, className }: { count: number; className?: string }) {
  return (
    <span
      className={cn(
        "pointer-events-none min-w-3.5 rounded-full bg-warning px-0.5 text-center text-3xs leading-3.5 font-semibold tabular-nums text-background",
        className,
      )}
    >
      {count}
    </span>
  );
}

/**
 * Fork: switches the sidebar between Threads and each thread group's workspace, such as
 * Headquarters. With one group a click flips between the two; with more it opens a menu. The badge
 * counts threads elsewhere that wait on the user.
 */
export function SidebarWorkspaceSwitch({
  names,
  workspace,
  waiting,
  onChange,
}: {
  names: readonly string[];
  workspace: SidebarWorkspace;
  waiting: ReadonlyMap<SidebarWorkspace, number>;
  onChange: (workspace: SidebarWorkspace) => void;
}) {
  const workspaces: SidebarWorkspace[] = [null, ...names];
  const waitingElsewhere = [...waiting].reduce(
    (total, [other, count]) => (other === workspace ? total : total + count),
    0,
  );
  const icon = workspace === null ? <LayersIcon /> : <Building2Icon />;
  const badge =
    waitingElsewhere > 0 ? (
      <WaitingBadge count={waitingElsewhere} className="absolute -top-0.5 -right-0.5" />
    ) : null;
  const waitingNote =
    waitingElsewhere > 0
      ? ` (${waitingElsewhere} waiting on you in ${workspaces
          .filter((other) => other !== workspace && (waiting.get(other) ?? 0) > 0)
          .map(labelOf)
          .join(", ")})`
      : "";

  if (names.length === 1) {
    const next = workspace === null ? names[0]! : null;
    return (
      <SidebarHeaderIconButton
        label={`Workspace: ${labelOf(workspace)}. Switch to ${labelOf(next)}${waitingNote}`}
        aria-pressed={workspace !== null}
        data-testid="sidebar-workspace-switch"
        isActive={workspace !== null}
        onClick={() => onChange(next)}
      >
        {icon}
        {badge}
      </SidebarHeaderIconButton>
    );
  }

  return (
    <Menu>
      <MenuTrigger
        render={
          <SidebarHeaderIconButton
            label={`Workspace: ${labelOf(workspace)}${waitingNote}`}
            data-testid="sidebar-workspace-switch"
            isActive={workspace !== null}
          />
        }
      >
        {icon}
        {badge}
      </MenuTrigger>
      <MenuPopup align="end" width="content">
        <MenuRadioGroup
          value={workspace ?? THREADS_VALUE}
          onValueChange={(value: string) => onChange(value === THREADS_VALUE ? null : value)}
        >
          {workspaces.map((option) => {
            const count = option === workspace ? 0 : (waiting.get(option) ?? 0);
            return (
              <MenuRadioItem key={option ?? THREADS_VALUE} value={option ?? THREADS_VALUE}>
                <span className="flex items-center gap-2">
                  {option === null ? <LayersIcon /> : <Building2Icon />}
                  <span className="flex-1">{labelOf(option)}</span>
                  {count > 0 ? <WaitingBadge count={count} /> : null}
                </span>
              </MenuRadioItem>
            );
          })}
        </MenuRadioGroup>
      </MenuPopup>
    </Menu>
  );
}
