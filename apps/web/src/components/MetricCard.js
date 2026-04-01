import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
export function MetricCard({ label, value, secondary, hint, title, wide = false }) {
    return (_jsxs("article", { className: wide ? "metric-card metric-card--wide" : "metric-card", title: title, children: [_jsx("span", { className: "metric-card__label", children: label }), _jsx("strong", { className: "metric-card__value", children: value }), secondary ? _jsx("span", { className: "metric-card__secondary", children: secondary }) : null, hint ? _jsx("span", { className: "metric-card__hint", children: hint }) : null] }));
}
