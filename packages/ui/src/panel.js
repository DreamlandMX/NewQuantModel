import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
export function Panel({ children, title, eyebrow }) {
    return (_jsxs("section", { className: "panel", children: [_jsxs("header", { className: "panel__header", children: [eyebrow ? _jsx("span", { className: "panel__eyebrow", children: eyebrow }) : null, _jsx("h2", { children: title })] }), _jsx("div", { className: "panel__body", children: children })] }));
}
