import {
  memo,
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  type KeyboardEvent,
  type PointerEvent,
} from "react";
import {
  MousePointer2Icon,
  PenLineIcon,
  SquareIcon,
  TypeIcon,
  Undo2Icon,
  type LucideIcon,
} from "lucide-react";

import { cn } from "~/lib/utils";
import { Button } from "../ui/button";
import { Dialog, DialogPopup, DialogTitle } from "../ui/dialog";
import { ToggleGroup, ToggleGroupItem } from "../ui/toggle-group";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { composerFloatingLayerProps } from "./composerEventScope";
import {
  ANNOTATION_COLORS,
  annotationFont,
  annotationFontSize,
  annotationStrokeWidth,
  drawAnnotations,
  isVisibleAnnotation,
  renderAnnotatedImage,
  type AnnotationPoint,
  type AnnotationTool,
  type ImageAnnotation,
} from "./imageAnnotations";

const TOOLS: ReadonlyArray<{ value: AnnotationTool; label: string; icon: LucideIcon }> = [
  { value: "pen", label: "Draw", icon: PenLineIcon },
  { value: "arrow", label: "Arrow", icon: MousePointer2Icon },
  { value: "rect", label: "Box", icon: SquareIcon },
  { value: "text", label: "Text", icon: TypeIcon },
];

function isAnnotationTool(value: unknown): value is AnnotationTool {
  return TOOLS.some((tool) => tool.value === value);
}

interface PendingText {
  readonly at: AnnotationPoint;
  readonly value: string;
}

interface ImageAnnotationDialogProps {
  readonly src: string;
  readonly name: string;
  /** Receives the flattened PNG; the dialog stays open until the promise settles. */
  readonly onSave: (blob: Blob) => Promise<void>;
  readonly onClose: () => void;
}

export const ImageAnnotationDialog = memo(function ImageAnnotationDialog({
  src,
  name,
  onSave,
  onClose,
}: ImageAnnotationDialogProps) {
  const imageRef = useRef<HTMLImageElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const textInputRef = useRef<HTMLInputElement | null>(null);
  const [imageSize, setImageSize] = useState<{ width: number; height: number } | null>(null);
  const [displayScale, setDisplayScale] = useState(1);
  const [tool, setTool] = useState<AnnotationTool>("arrow");
  const [color, setColor] = useState<string>(ANNOTATION_COLORS[0].value);
  const [annotations, setAnnotations] = useState<ReadonlyArray<ImageAnnotation>>([]);
  const [draft, setDraft] = useState<ImageAnnotation | null>(null);
  const [pendingText, setPendingTextState] = useState<PendingText | null>(null);
  // Blur and a click elsewhere can both commit the same text; the ref lets only the first win.
  const pendingTextRef = useRef<PendingText | null>(null);
  const setPendingText = (next: PendingText | null) => {
    pendingTextRef.current = next;
    setPendingTextState(next);
  };
  const [saveState, setSaveState] = useState<
    { status: "idle" } | { status: "saving" } | { status: "failed"; message: string }
  >({ status: "idle" });

  const strokeWidth = imageSize ? annotationStrokeWidth(imageSize.width, imageSize.height) : 0;
  const fontSize = annotationFontSize(strokeWidth);

  useLayoutEffect(() => {
    const canvas = canvasRef.current;
    const ctx = canvas?.getContext("2d");
    // Resizing the canvas to a newly loaded image wipes it, so redraw then too.
    if (!canvas || !ctx || !imageSize) return;
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    drawAnnotations(ctx, draft ? [...annotations, draft] : annotations);
  }, [annotations, draft, imageSize]);

  // The text input is drawn in screen pixels, so it tracks how far the image is scaled down.
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !imageSize) return;
    const observer = new ResizeObserver(() => {
      const width = canvas.getBoundingClientRect().width;
      if (width > 0) setDisplayScale(width / imageSize.width);
    });
    observer.observe(canvas);
    return () => observer.disconnect();
  }, [imageSize]);

  const toImagePoint = (event: PointerEvent<HTMLCanvasElement>): AnnotationPoint => {
    const rect = event.currentTarget.getBoundingClientRect();
    const size = imageSize ?? { width: rect.width, height: rect.height };
    return {
      x: ((event.clientX - rect.left) / rect.width) * size.width,
      y: ((event.clientY - rect.top) / rect.height) * size.height,
    };
  };

  const commitPendingText = () => {
    const current = pendingTextRef.current;
    if (!current) return;
    setPendingText(null);
    const annotation: ImageAnnotation = {
      kind: "text",
      color,
      fontSize,
      at: current.at,
      text: current.value,
    };
    if (isVisibleAnnotation(annotation)) {
      setAnnotations((existing) => [...existing, annotation]);
    }
  };

  const onPointerDown = (event: PointerEvent<HTMLCanvasElement>) => {
    if (event.button !== 0 || saveState.status === "saving") return;
    event.preventDefault();
    const point = toImagePoint(event);
    if (tool === "text") {
      commitPendingText();
      setPendingText({ at: point, value: "" });
      return;
    }
    event.currentTarget.setPointerCapture(event.pointerId);
    setDraft(
      tool === "pen"
        ? { kind: "pen", color, width: strokeWidth, points: [point] }
        : { kind: tool, color, width: strokeWidth, from: point, to: point },
    );
  };

  const onPointerMove = (event: PointerEvent<HTMLCanvasElement>) => {
    if (!draft) return;
    const point = toImagePoint(event);
    setDraft((current) => {
      if (!current) return current;
      switch (current.kind) {
        case "pen":
          return { ...current, points: [...current.points, point] };
        case "arrow":
        case "rect":
          return { ...current, to: point };
        case "text":
          return current;
      }
    });
  };

  const onPointerUp = () => {
    if (!draft) return;
    if (isVisibleAnnotation(draft)) setAnnotations((existing) => [...existing, draft]);
    setDraft(null);
  };

  const undo = useCallback(() => {
    setAnnotations((existing) => existing.slice(0, -1));
  }, []);

  const save = async () => {
    const image = imageRef.current;
    if (!image || saveState.status === "saving") return;
    const currentText = pendingTextRef.current;
    const pending: ImageAnnotation | null = currentText
      ? { kind: "text", color, fontSize, at: currentText.at, text: currentText.value }
      : null;
    const finalAnnotations =
      pending && isVisibleAnnotation(pending) ? [...annotations, pending] : annotations;
    if (finalAnnotations.length === 0) {
      onClose();
      return;
    }
    setPendingText(null);
    setAnnotations(finalAnnotations);
    setSaveState({ status: "saving" });
    try {
      await onSave(await renderAnnotatedImage(image, finalAnnotations));
    } catch (error) {
      setSaveState({
        status: "failed",
        message: error instanceof Error ? error.message : "The edited image could not be saved.",
      });
    }
  };

  // Escape and undo are captured at the window so they win over the composer's own shortcuts.
  useEffect(() => {
    const onKey = (event: globalThis.KeyboardEvent) => {
      const inTextInput = event.target === textInputRef.current;
      if (event.key === "Escape") {
        event.preventDefault();
        event.stopPropagation();
        if (inTextInput) {
          pendingTextRef.current = null;
          setPendingTextState(null);
        } else {
          onClose();
        }
        return;
      }
      if (inTextInput) return;
      if (event.key.toLowerCase() === "z" && (event.metaKey || event.ctrlKey) && !event.shiftKey) {
        event.preventDefault();
        event.stopPropagation();
        undo();
      }
    };
    window.addEventListener("keydown", onKey, { capture: true });
    return () => window.removeEventListener("keydown", onKey, { capture: true });
  }, [onClose, undo]);

  const onTextKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    event.stopPropagation();
    if (event.key === "Enter") {
      event.preventDefault();
      commitPendingText();
    }
  };

  const saving = saveState.status === "saving";

  return (
    <Dialog
      open
      disablePointerDismissal
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
    >
      <DialogPopup
        {...composerFloatingLayerProps}
        variant="media"
        showCloseButton={false}
        bottomStickOnMobile={false}
        className="row-start-1 max-h-[92vh] w-[92vw] max-w-[92vw] items-center overflow-visible"
      >
        <DialogTitle className="sr-only">Edit {name}</DialogTitle>
        <div className="flex max-w-full flex-col items-center gap-3">
          <div className="flex flex-wrap items-center justify-center gap-2 rounded-xl border border-border/70 bg-popover p-1.5 text-popover-foreground shadow-2xl">
            <ToggleGroup
              aria-label="Annotation tool"
              value={[tool]}
              onValueChange={(next) => {
                const value = next[0];
                if (isAnnotationTool(value)) setTool(value);
              }}
            >
              {TOOLS.map(({ value, label, icon: Icon }) => (
                <ToggleGroupItem key={value} value={value} aria-label={label}>
                  <Icon className="size-3.5" />
                  {label}
                </ToggleGroupItem>
              ))}
            </ToggleGroup>
            <div
              role="radiogroup"
              aria-label="Annotation color"
              className="flex items-center gap-1"
            >
              {ANNOTATION_COLORS.map((option) => (
                <button
                  key={option.value}
                  type="button"
                  role="radio"
                  aria-checked={color === option.value}
                  aria-label={option.label}
                  className={cn(
                    "size-5 cursor-pointer rounded-full border border-black/30 outline-none ring-offset-2 ring-offset-popover transition-shadow focus-visible:ring-2 focus-visible:ring-ring",
                    color === option.value && "ring-2 ring-foreground/70",
                  )}
                  style={{ backgroundColor: option.value }}
                  onClick={() => setColor(option.value)}
                />
              ))}
            </div>
            <Tooltip>
              <TooltipTrigger
                render={
                  <Button
                    size="icon-sm"
                    variant="ghost"
                    aria-label="Undo"
                    disabled={annotations.length === 0 || saving}
                    onClick={undo}
                  />
                }
              >
                <Undo2Icon />
              </TooltipTrigger>
              <TooltipPopup side="bottom">Undo</TooltipPopup>
            </Tooltip>
            <Button size="sm" variant="ghost" disabled={saving} onClick={onClose}>
              Cancel
            </Button>
            <Button size="sm" disabled={saving || imageSize === null} onClick={() => void save()}>
              {saving ? "Saving…" : "Done"}
            </Button>
          </div>
          {saveState.status === "failed" ? (
            <p role="alert" className="text-xs text-white">
              {saveState.message}
            </p>
          ) : null}
          <div className="relative max-h-[min(80vh,calc(100vh-180px))] max-w-full">
            <img
              ref={imageRef}
              src={src}
              alt={name}
              draggable={false}
              className="block max-h-[min(80vh,calc(100vh-180px))] max-w-full select-none rounded-lg border border-border/70 shadow-2xl"
              onLoad={(event) =>
                setImageSize({
                  width: event.currentTarget.naturalWidth,
                  height: event.currentTarget.naturalHeight,
                })
              }
            />
            {imageSize ? (
              <canvas
                ref={canvasRef}
                width={imageSize.width}
                height={imageSize.height}
                aria-label="Annotation canvas"
                className={cn(
                  "absolute inset-0 size-full touch-none rounded-lg",
                  tool === "text" ? "cursor-text" : "cursor-crosshair",
                )}
                onPointerDown={onPointerDown}
                onPointerMove={onPointerMove}
                onPointerUp={onPointerUp}
                onPointerCancel={onPointerUp}
              />
            ) : null}
            {pendingText && imageSize ? (
              <input
                ref={(node) => {
                  textInputRef.current = node;
                  node?.focus({ preventScroll: true });
                }}
                aria-label="Annotation text"
                value={pendingText.value}
                placeholder="Type, then press Enter"
                className="absolute min-w-40 border-0 bg-transparent p-0 outline-none placeholder:text-white/60 [paint-order:stroke] [-webkit-text-stroke:0.2em_rgba(0,0,0,0.7)]"
                style={{
                  left: `${String((pendingText.at.x / imageSize.width) * 100)}%`,
                  top: `${String((pendingText.at.y / imageSize.height) * 100)}%`,
                  color,
                  font: annotationFont(fontSize * displayScale),
                  lineHeight: 1,
                }}
                onChange={(event) => {
                  const current = pendingTextRef.current;
                  if (current) setPendingText({ ...current, value: event.target.value });
                }}
                onKeyDown={onTextKeyDown}
                onBlur={commitPendingText}
              />
            ) : null}
          </div>
        </div>
      </DialogPopup>
    </Dialog>
  );
});
