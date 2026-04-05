import type { ReactNode } from "react";

export function MetricCard({
  label,
  value,
  secondary,
  hint,
  title,
  wide = false,
  tone = "neutral",
  compact = false
}: {
  label: string;
  value: ReactNode;
  secondary?: ReactNode;
  hint?: ReactNode;
  title?: string;
  wide?: boolean;
  tone?: "neutral" | "positive" | "negative" | "accent";
  compact?: boolean;
}) {
  const className = [
    "metric-card",
    wide ? "metric-card--wide" : "",
    compact ? "metric-card--compact" : "",
    tone !== "neutral" ? `metric-card--${tone}` : ""
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <article className={className} title={title}>
      <span className="metric-card__label">{label}</span>
      <strong className="metric-card__value">{value}</strong>
      {secondary ? <span className="metric-card__secondary">{secondary}</span> : null}
      {hint ? <span className="metric-card__hint">{hint}</span> : null}
    </article>
  );
}
