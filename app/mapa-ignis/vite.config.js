import { defineConfig } from 'vite';

export default defineConfig({
  envDir: '../../',
  server: {
    proxy: {
      '/predict': 'http://localhost:8000',
      '/imagen': 'http://localhost:8000',
      '/info_incendios': 'http://localhost:8000',
      '/info_frp': 'http://localhost:8000'
    }
  }
});
