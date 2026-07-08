import { defineConfig } from "astro/config";
import rangefind from "../../src/index.js";

export default defineConfig({
  integrations: [rangefind()]
});
