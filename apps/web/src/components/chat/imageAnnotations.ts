export interface AnnotationPoint {
  readonly x: number;
  readonly y: number;
}

export type AnnotationTool = "pen" | "arrow" | "rect" | "text";

export type ImageAnnotation =
  | {
      readonly kind: "pen";
      readonly color: string;
      readonly width: number;
      readonly points: ReadonlyArray<AnnotationPoint>;
    }
  | {
      readonly kind: "arrow" | "rect";
      readonly color: string;
      readonly width: number;
      readonly from: AnnotationPoint;
      readonly to: AnnotationPoint;
    }
  | {
      readonly kind: "text";
      readonly color: string;
      readonly fontSize: number;
      readonly at: AnnotationPoint;
      readonly text: string;
    };

/** Pixel colors burned into the image, so they stay the same in every theme. */
export const ANNOTATION_COLORS = [
  { label: "Red", value: "#ef4444" },
  { label: "Yellow", value: "#facc15" },
  { label: "Green", value: "#22c55e" },
  { label: "Blue", value: "#3b82f6" },
  { label: "White", value: "#ffffff" },
] as const;

const STROKE_WIDTH_PER_IMAGE_EDGE = 1 / 320;
const MIN_STROKE_WIDTH = 2;
const FONT_SIZE_PER_STROKE_WIDTH = 6;
const ARROW_HEAD_LENGTH_PER_STROKE_WIDTH = 4.5;
const ARROW_HEAD_ANGLE = Math.PI / 7;
const ANNOTATION_FONT_FAMILY = "system-ui, -apple-system, sans-serif";

/** Strokes scale with the image so a mark reads the same on a small or a 4K capture. */
export function annotationStrokeWidth(imageWidth: number, imageHeight: number): number {
  return Math.max(
    MIN_STROKE_WIDTH,
    Math.round(Math.max(imageWidth, imageHeight) * STROKE_WIDTH_PER_IMAGE_EDGE),
  );
}

export function annotationFontSize(strokeWidth: number): number {
  return strokeWidth * FONT_SIZE_PER_STROKE_WIDTH;
}

export function annotationFont(fontSize: number): string {
  return `600 ${String(fontSize)}px ${ANNOTATION_FONT_FAMILY}`;
}

/** A click without a drag leaves a zero-size box or arrow; those are not worth keeping. */
export function isVisibleAnnotation(annotation: ImageAnnotation): boolean {
  switch (annotation.kind) {
    case "pen":
      return annotation.points.length > 0;
    case "arrow":
    case "rect":
      return (
        Math.hypot(annotation.to.x - annotation.from.x, annotation.to.y - annotation.from.y) >=
        annotation.width
      );
    case "text":
      return annotation.text.trim().length > 0;
  }
}

function drawPen(
  ctx: CanvasRenderingContext2D,
  annotation: Extract<ImageAnnotation, { kind: "pen" }>,
) {
  const [first, ...rest] = annotation.points;
  if (!first) return;
  ctx.beginPath();
  ctx.moveTo(first.x, first.y);
  if (rest.length === 0) {
    // A single tap still leaves a dot.
    ctx.lineTo(first.x + 0.01, first.y);
  }
  for (const point of rest) ctx.lineTo(point.x, point.y);
  ctx.stroke();
}

function drawArrow(
  ctx: CanvasRenderingContext2D,
  annotation: Extract<ImageAnnotation, { kind: "arrow" | "rect" }>,
) {
  const { from, to, width } = annotation;
  const angle = Math.atan2(to.y - from.y, to.x - from.x);
  const headLength = width * ARROW_HEAD_LENGTH_PER_STROKE_WIDTH;
  ctx.beginPath();
  ctx.moveTo(from.x, from.y);
  ctx.lineTo(to.x, to.y);
  ctx.stroke();
  ctx.beginPath();
  ctx.moveTo(to.x, to.y);
  ctx.lineTo(
    to.x - headLength * Math.cos(angle - ARROW_HEAD_ANGLE),
    to.y - headLength * Math.sin(angle - ARROW_HEAD_ANGLE),
  );
  ctx.lineTo(
    to.x - headLength * Math.cos(angle + ARROW_HEAD_ANGLE),
    to.y - headLength * Math.sin(angle + ARROW_HEAD_ANGLE),
  );
  ctx.closePath();
  ctx.fill();
  ctx.stroke();
}

function drawText(
  ctx: CanvasRenderingContext2D,
  annotation: Extract<ImageAnnotation, { kind: "text" }>,
) {
  ctx.font = annotationFont(annotation.fontSize);
  ctx.textBaseline = "top";
  // A dark halo keeps light and dark text readable on any background.
  ctx.lineWidth = annotation.fontSize / 5;
  ctx.strokeStyle = "rgba(0, 0, 0, 0.7)";
  const lineHeight = annotation.fontSize * 1.2;
  annotation.text.split("\n").forEach((line, index) => {
    const y = annotation.at.y + index * lineHeight;
    ctx.strokeText(line, annotation.at.x, y);
    ctx.fillText(line, annotation.at.x, y);
  });
}

export function drawAnnotations(
  ctx: CanvasRenderingContext2D,
  annotations: ReadonlyArray<ImageAnnotation>,
): void {
  for (const annotation of annotations) {
    ctx.save();
    ctx.lineCap = "round";
    ctx.lineJoin = "round";
    ctx.strokeStyle = annotation.color;
    ctx.fillStyle = annotation.color;
    switch (annotation.kind) {
      case "pen":
        ctx.lineWidth = annotation.width;
        drawPen(ctx, annotation);
        break;
      case "arrow":
        ctx.lineWidth = annotation.width;
        drawArrow(ctx, annotation);
        break;
      case "rect":
        ctx.lineWidth = annotation.width;
        ctx.strokeRect(
          annotation.from.x,
          annotation.from.y,
          annotation.to.x - annotation.from.x,
          annotation.to.y - annotation.from.y,
        );
        break;
      case "text":
        drawText(ctx, annotation);
        break;
    }
    ctx.restore();
  }
}

/** Flattens the marks onto the image at its full resolution as a PNG. */
export async function renderAnnotatedImage(
  image: HTMLImageElement,
  annotations: ReadonlyArray<ImageAnnotation>,
): Promise<Blob> {
  const canvas = document.createElement("canvas");
  canvas.width = image.naturalWidth;
  canvas.height = image.naturalHeight;
  const ctx = canvas.getContext("2d");
  if (!ctx) throw new Error("Canvas is unavailable.");
  ctx.drawImage(image, 0, 0);
  drawAnnotations(ctx, annotations);
  return new Promise((resolve, reject) => {
    canvas.toBlob((blob) => {
      if (blob) resolve(blob);
      else reject(new Error("The edited image could not be encoded."));
    }, "image/png");
  });
}

export function pngFileName(name: string): string {
  const base = name.replace(/\.[^./\\]+$/, "");
  return `${base.length > 0 ? base : "image"}.png`;
}
