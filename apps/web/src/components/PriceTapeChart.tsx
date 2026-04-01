import { useEffect, useRef } from "react";
import { ColorType, createChart } from "lightweight-charts";

import type { ForecastRecord } from "@newquantmodel/shared-types";

export function PriceTapeChart({ items }: { items: ForecastRecord[] }) {
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!containerRef.current) {
      return;
    }

    const chart = createChart(containerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "#091217" },
        textColor: "#A7BBC7"
      },
      rightPriceScale: {
        borderColor: "rgba(167,187,199,0.15)"
      },
      timeScale: {
        borderColor: "rgba(167,187,199,0.15)"
      },
      grid: {
        vertLines: { color: "rgba(167,187,199,0.06)" },
        horzLines: { color: "rgba(167,187,199,0.06)" }
      }
    });

    const lineSeries = chart.addLineSeries({
      color: "#4fc3f7",
      lineWidth: 2
    });

    lineSeries.setData(
      items.slice(0, 24).map((item, index) => ({
        time: `2026-03-${String(index + 1).padStart(2, "0")}`,
        value: 100 + item.q50 * 100
      }))
    );

    chart.timeScale().fitContent();
    return () => chart.remove();
  }, [items]);

  return <div className="tape-chart" ref={containerRef} />;
}
