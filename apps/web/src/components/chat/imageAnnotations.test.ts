import { describe, expect, it } from "vitest";

import { annotationStrokeWidth, isVisibleAnnotation, pngFileName } from "./imageAnnotations";

describe("imageAnnotations", () => {
  it("scales strokes with the image's longest edge", () => {
    expect(annotationStrokeWidth(100, 80)).toBe(2);
    expect(annotationStrokeWidth(1470, 866)).toBe(5);
    expect(annotationStrokeWidth(2160, 3840)).toBe(12);
  });

  it("drops clicks that never became a mark", () => {
    const point = { x: 10, y: 10 };
    expect(
      isVisibleAnnotation({ kind: "rect", color: "red", width: 4, from: point, to: point }),
    ).toBe(false);
    expect(
      isVisibleAnnotation({
        kind: "arrow",
        color: "red",
        width: 4,
        from: point,
        to: { x: 40, y: 10 },
      }),
    ).toBe(true);
    expect(
      isVisibleAnnotation({ kind: "text", color: "red", fontSize: 24, at: point, text: "  " }),
    ).toBe(false);
    expect(isVisibleAnnotation({ kind: "pen", color: "red", width: 4, points: [point] })).toBe(
      true,
    );
  });

  it("names the flattened file as a PNG", () => {
    expect(pngFileName("window.png")).toBe("window.png");
    expect(pngFileName("photo.final.jpeg")).toBe("photo.final.png");
    expect(pngFileName("notes")).toBe("notes.png");
    expect(pngFileName(".jpg")).toBe("image.png");
  });
});
