import type { ReactNode } from "react";

export function ValueBlock({
  label,
  primary,
  secondary,
  tertiary,
  title,
  className,
  tone = "neutral"
}: {
  label: string;
  primary: ReactNode;
  secondary?: ReactNode;
  tertiary?: ReactNode;
  title?: string;
  className?: string;
  tone?: "neutral" | "positive" | "negative" | "accent" | "muted";
}) {
  const classes = ["value-block", className, tone !== "neutral" ? `value-block--${tone}` : ""].filter(Boolean).join(" ");
  return (
    <article className={classes} title={title}>
      <span className="value-block__label">{label}</span>
      <strong className="value-block__primary">{primary}</strong>
      {secondary ? <span className="value-block__secondary">{secondary}</span> : null}
      {tertiary ? <span className="value-block__tertiary">{tertiary}</span> : null}
    </article>
  );
}

export function ChipList({ items }: { items: Array<{ label: string; title?: string }> }) {
  return (
    <div className="chip-list">
      {items.map((item) => (
        <span className="chip-list__item" key={`${item.label}-${item.title ?? ""}`} title={item.title}>
          {item.label}
        </span>
      ))}
    </div>
  );
}
