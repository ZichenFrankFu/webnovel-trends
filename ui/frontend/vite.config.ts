// ui/frontend/vite.config.ts
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

export default defineConfig(({ mode }) => {
  const isDev = mode === "development";

  return {
    plugins: [react()],

    // 重要：让 build 后的 index.html 用相对路径加载 assets
    // 否则 FastAPI 托管静态文件时很容易出现资源 404
    base: "./",

    resolve: {
      alias: {
        "@": path.resolve(__dirname, "src"),
      },
    },

    server: {
      host: "127.0.0.1",
      port: 5173,
      strictPort: true,

      // 开发时前端直接 /api 调后端（避免 CORS）
      proxy: {
        "/api": {
          target: "http://127.0.0.1:8713",
          changeOrigin: true,
        },
      },
    },

    build: {
      // 输出到后端可托管目录：ui/backend/app/static
      outDir: path.resolve(__dirname, "../backend/app/static"),
      emptyOutDir: true,

      // 默认就是 assets，你也可以显式写上，方便后端挂载 /assets
      assetsDir: "assets",

      sourcemap: false,

      // 可选：避免某些依赖打包后出现大 chunk 警告（不影响功能）
      chunkSizeWarningLimit: 1500,

      // 保险：明确入口
      rollupOptions: {
        input: path.resolve(__dirname, "index.html"),
      },
    },
  };
});
