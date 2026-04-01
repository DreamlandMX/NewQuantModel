import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "0.0.0.0",
    port: 5173
  },
  build: {
    chunkSizeWarningLimit: 900,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes("node_modules/react") || id.includes("node_modules/react-dom")) {
            return "react";
          }
          if (id.includes("@tanstack/react-query")) {
            return "query";
          }
          if (id.includes("node_modules/echarts/")) {
            return "echarts-core";
          }
          if (id.includes("echarts-for-react")) {
            return "echarts-react";
          }
          if (id.includes("lightweight-charts")) {
            return "lightweight-charts";
          }
        }
      }
    }
  }
});
