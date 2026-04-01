import type { PropsWithChildren } from "react";

export function Panel({ children, title, eyebrow }: PropsWithChildren<{ title: string; eyebrow?: string }>) {
  return (
    <section className="panel">
      <header className="panel__header">
        {eyebrow ? <span className="panel__eyebrow">{eyebrow}</span> : null}
        <h2>{title}</h2>
      </header>
      <div className="panel__body">{children}</div>
    </section>
  );
}
