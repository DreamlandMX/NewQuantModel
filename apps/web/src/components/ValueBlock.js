import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
export function ValueBlock({ label, primary, secondary, tertiary, title, className }) {
    return (_jsxs("article", { className: className ? `value-block ${className}` : "value-block", title: title, children: [_jsx("span", { className: "value-block__label", children: label }), _jsx("strong", { className: "value-block__primary", children: primary }), secondary ? _jsx("span", { className: "value-block__secondary", children: secondary }) : null, tertiary ? _jsx("span", { className: "value-block__tertiary", children: tertiary }) : null] }));
}
export function ChipList({ items }) {
    return (_jsx("div", { className: "chip-list", children: items.map((item) => (_jsx("span", { className: "chip-list__item", title: item.title, children: item.label }, `${item.label}-${item.title ?? ""}`))) }));
}
