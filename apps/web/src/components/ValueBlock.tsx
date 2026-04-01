import type { ReactNode } from "react";

export function ValueBlock({
  label,
  primary,
  secondary,
  tertiary,
  title,
  className
}: {
  label: string;
  primary: ReactNode;
  secondary?: ReactNode;
  tertiary?: ReactNode;
  title?: string;
  className?: string;
}) {
  return (
    <article className={className ? `value-block ${className}` : "value-block"} title={title}>
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
