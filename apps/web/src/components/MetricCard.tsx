import type { ReactNode } from "react";

export function MetricCard({
  label,
  value,
  secondary,
  hint,
  title,
  wide = false
}: {
  label: string;
  value: ReactNode;
  secondary?: ReactNode;
  hint?: ReactNode;
  title?: string;
  wide?: boolean;
}) {
  return (
    <article className={wide ? "metric-card metric-card--wide" : "metric-card"} title={title}>
      <span className="metric-card__label">{label}</span>
      <strong className="metric-card__value">{value}</strong>
      {secondary ? <span className="metric-card__secondary">{secondary}</span> : null}
      {hint ? <span className="metric-card__hint">{hint}</span> : null}
    </article>
  );
}
