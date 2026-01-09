// docs/.vitepress/config.mjs
import { defineConfig } from "file:///opt/sca/app/node_modules/vitepress/dist/node/index.js";
import { withMermaid } from "file:///opt/sca/app/node_modules/vitepress-plugin-mermaid/dist/vitepress-plugin-mermaid.es.mjs";
import { withSidebar } from "file:///opt/sca/app/node_modules/vitepress-sidebar/dist/index.js";
var vitePressOptions = {
  title: "Bioloop",
  base: "/bioloop/",
  description: "Bioloop Documentation",
  head: [["link", { rel: "icon", href: "/bioloop/docs/favicon.ico" }]],
  lastUpdated: true,
  // Enable last updated timestamp
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    nav: [
      { text: "Home", link: "/" },
      { text: "UI", link: "/ui/overview" },
      { text: "API", link: "/api/introduction" },
      { text: "Workers", link: "/worker/overview" }
    ],
    socialLinks: [
      { icon: "github", link: "https://github.com/IUSCA/bioloop" }
    ],
    search: {
      provider: "local"
    },
    editLink: {
      pattern: "https://github.com/IUSCA/bioloop/edit/main/docs/:path"
    }
  },
  ignoreDeadLinks: [
    // ignore exact url "/playground"
    "/playground",
    // ignore all localhost links
    /^https?:\/\/localhost/,
    // ignore all links include "/repl/""
    /\/repl\//,
    // custom function, ignore all links include "ignore"
    (url) => {
      return url.toLowerCase().includes("ignore");
    }
  ]
};
vitePressOptions = withMermaid({
  ...vitePressOptions,
  mermaid: {
    // refer https://mermaid.js.org/config/setup/modules/mermaidAPI.html#mermaidapi-configuration-defaults for options
  },
  // optionally set additional config for plugin itself with MermaidPluginConfig
  mermaidPlugin: {
    class: "mermaid my-class"
    // set additional css classes for parent container 
  }
});
var vitePressSidebarOptions = {
  // VitePress Sidebar's options here...
  documentRootPath: "/docs",
  collapsed: true,
  capitalizeFirst: true,
  includeFolderIndexFile: false,
  useTitleFromFileHeading: true,
  useTitleFromFrontmatter: true,
  useFolderTitleFromIndexFile: true,
  frontmatterOrderDefaultValue: 100,
  sortMenusByFrontmatterOrder: true,
  excludeFilesByFrontmatterFieldName: "exclude"
};
var config_default = defineConfig(withSidebar(vitePressOptions, vitePressSidebarOptions));
export {
  config_default as default
};
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsiZG9jcy8udml0ZXByZXNzL2NvbmZpZy5tanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvb3B0L3NjYS9hcHAvZG9jcy8udml0ZXByZXNzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvb3B0L3NjYS9hcHAvZG9jcy8udml0ZXByZXNzL2NvbmZpZy5tanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL29wdC9zY2EvYXBwL2RvY3MvLnZpdGVwcmVzcy9jb25maWcubWpzXCI7aW1wb3J0IHsgZGVmaW5lQ29uZmlnIH0gZnJvbSAndml0ZXByZXNzJztcbmltcG9ydCB7IHdpdGhNZXJtYWlkIH0gZnJvbSBcInZpdGVwcmVzcy1wbHVnaW4tbWVybWFpZFwiO1xuaW1wb3J0IHsgd2l0aFNpZGViYXIgfSBmcm9tICd2aXRlcHJlc3Mtc2lkZWJhcic7XG5cbi8vIGh0dHBzOi8vdml0ZXByZXNzLmRldi9yZWZlcmVuY2Uvc2l0ZS1jb25maWdcbmxldCB2aXRlUHJlc3NPcHRpb25zID0gIHtcbiAgdGl0bGU6IFwiQmlvbG9vcFwiLFxuICBiYXNlOiBcIi9iaW9sb29wL1wiLFxuICBkZXNjcmlwdGlvbjogXCJCaW9sb29wIERvY3VtZW50YXRpb25cIixcbiAgaGVhZDogW1snbGluaycsIHsgcmVsOiAnaWNvbicsIGhyZWY6ICcvYmlvbG9vcC9kb2NzL2Zhdmljb24uaWNvJyB9XV0sXG4gIGxhc3RVcGRhdGVkOiB0cnVlLCAvLyBFbmFibGUgbGFzdCB1cGRhdGVkIHRpbWVzdGFtcFxuICB0aGVtZUNvbmZpZzoge1xuICAgIC8vIGh0dHBzOi8vdml0ZXByZXNzLmRldi9yZWZlcmVuY2UvZGVmYXVsdC10aGVtZS1jb25maWdcbiAgICBuYXY6IFtcbiAgICAgIHsgdGV4dDogJ0hvbWUnLCBsaW5rOiAnLycgfSxcbiAgICAgIHsgdGV4dDogJ1VJJywgbGluazogJy91aS9vdmVydmlldycgfSxcbiAgICAgIHsgdGV4dDogJ0FQSScsIGxpbms6ICcvYXBpL2ludHJvZHVjdGlvbicgfSxcbiAgICAgIHsgdGV4dDogJ1dvcmtlcnMnLCBsaW5rOiAnL3dvcmtlci9vdmVydmlldycgfSxcblxuICAgIF0sXG5cbiAgICBzb2NpYWxMaW5rczogW1xuICAgICAgeyBpY29uOiAnZ2l0aHViJywgbGluazogJ2h0dHBzOi8vZ2l0aHViLmNvbS9JVVNDQS9iaW9sb29wJyB9XG4gICAgXSxcblxuICAgIHNlYXJjaDoge1xuICAgICAgcHJvdmlkZXI6ICdsb2NhbCdcbiAgICB9LFxuXG4gICAgZWRpdExpbms6IHtcbiAgICAgIHBhdHRlcm46ICdodHRwczovL2dpdGh1Yi5jb20vSVVTQ0EvYmlvbG9vcC9lZGl0L21haW4vZG9jcy86cGF0aCdcbiAgICB9XG4gIH0sXG4gIGlnbm9yZURlYWRMaW5rczogW1xuICAgIC8vIGlnbm9yZSBleGFjdCB1cmwgXCIvcGxheWdyb3VuZFwiXG4gICAgJy9wbGF5Z3JvdW5kJyxcbiAgICAvLyBpZ25vcmUgYWxsIGxvY2FsaG9zdCBsaW5rc1xuICAgIC9eaHR0cHM/OlxcL1xcL2xvY2FsaG9zdC8sXG4gICAgLy8gaWdub3JlIGFsbCBsaW5rcyBpbmNsdWRlIFwiL3JlcGwvXCJcIlxuICAgIC9cXC9yZXBsXFwvLyxcbiAgICAvLyBjdXN0b20gZnVuY3Rpb24sIGlnbm9yZSBhbGwgbGlua3MgaW5jbHVkZSBcImlnbm9yZVwiXG4gICAgKHVybCkgPT4ge1xuICAgICAgcmV0dXJuIHVybC50b0xvd2VyQ2FzZSgpLmluY2x1ZGVzKCdpZ25vcmUnKVxuICAgIH1cbiAgXVxufTtcblxudml0ZVByZXNzT3B0aW9ucyA9IHdpdGhNZXJtYWlkKHtcbiAgLi4udml0ZVByZXNzT3B0aW9ucyxcbiAgbWVybWFpZDoge1xuICAgIC8vIHJlZmVyIGh0dHBzOi8vbWVybWFpZC5qcy5vcmcvY29uZmlnL3NldHVwL21vZHVsZXMvbWVybWFpZEFQSS5odG1sI21lcm1haWRhcGktY29uZmlndXJhdGlvbi1kZWZhdWx0cyBmb3Igb3B0aW9uc1xuICB9LFxuICAvLyBvcHRpb25hbGx5IHNldCBhZGRpdGlvbmFsIGNvbmZpZyBmb3IgcGx1Z2luIGl0c2VsZiB3aXRoIE1lcm1haWRQbHVnaW5Db25maWdcbiAgbWVybWFpZFBsdWdpbjoge1xuICAgIGNsYXNzOiBcIm1lcm1haWQgbXktY2xhc3NcIiwgLy8gc2V0IGFkZGl0aW9uYWwgY3NzIGNsYXNzZXMgZm9yIHBhcmVudCBjb250YWluZXIgXG4gIH0sXG59KVxuXG5cbmNvbnN0IHZpdGVQcmVzc1NpZGViYXJPcHRpb25zID0ge1xuICAvLyBWaXRlUHJlc3MgU2lkZWJhcidzIG9wdGlvbnMgaGVyZS4uLlxuICBkb2N1bWVudFJvb3RQYXRoOiAnL2RvY3MnLFxuICBjb2xsYXBzZWQ6IHRydWUsXG4gIGNhcGl0YWxpemVGaXJzdDogdHJ1ZSxcbiAgaW5jbHVkZUZvbGRlckluZGV4RmlsZTogZmFsc2UsXG4gIHVzZVRpdGxlRnJvbUZpbGVIZWFkaW5nOiB0cnVlLFxuICB1c2VUaXRsZUZyb21Gcm9udG1hdHRlcjogdHJ1ZSxcbiAgdXNlRm9sZGVyVGl0bGVGcm9tSW5kZXhGaWxlOiB0cnVlLFxuICBmcm9udG1hdHRlck9yZGVyRGVmYXVsdFZhbHVlOiAxMDAsXG4gIHNvcnRNZW51c0J5RnJvbnRtYXR0ZXJPcmRlcjogdHJ1ZSxcbiAgZXhjbHVkZUZpbGVzQnlGcm9udG1hdHRlckZpZWxkTmFtZTogJ2V4Y2x1ZGUnXG59O1xuXG5leHBvcnQgZGVmYXVsdCBkZWZpbmVDb25maWcod2l0aFNpZGViYXIodml0ZVByZXNzT3B0aW9ucywgdml0ZVByZXNzU2lkZWJhck9wdGlvbnMpKTtcbi8vIGV4cG9ydCBkZWZhdWx0IHZpdGVQcmVzc09wdGlvbnM7Il0sCiAgIm1hcHBpbmdzIjogIjtBQUE4UCxTQUFTLG9CQUFvQjtBQUMzUixTQUFTLG1CQUFtQjtBQUM1QixTQUFTLG1CQUFtQjtBQUc1QixJQUFJLG1CQUFvQjtBQUFBLEVBQ3RCLE9BQU87QUFBQSxFQUNQLE1BQU07QUFBQSxFQUNOLGFBQWE7QUFBQSxFQUNiLE1BQU0sQ0FBQyxDQUFDLFFBQVEsRUFBRSxLQUFLLFFBQVEsTUFBTSw0QkFBNEIsQ0FBQyxDQUFDO0FBQUEsRUFDbkUsYUFBYTtBQUFBO0FBQUEsRUFDYixhQUFhO0FBQUE7QUFBQSxJQUVYLEtBQUs7QUFBQSxNQUNILEVBQUUsTUFBTSxRQUFRLE1BQU0sSUFBSTtBQUFBLE1BQzFCLEVBQUUsTUFBTSxNQUFNLE1BQU0sZUFBZTtBQUFBLE1BQ25DLEVBQUUsTUFBTSxPQUFPLE1BQU0sb0JBQW9CO0FBQUEsTUFDekMsRUFBRSxNQUFNLFdBQVcsTUFBTSxtQkFBbUI7QUFBQSxJQUU5QztBQUFBLElBRUEsYUFBYTtBQUFBLE1BQ1gsRUFBRSxNQUFNLFVBQVUsTUFBTSxtQ0FBbUM7QUFBQSxJQUM3RDtBQUFBLElBRUEsUUFBUTtBQUFBLE1BQ04sVUFBVTtBQUFBLElBQ1o7QUFBQSxJQUVBLFVBQVU7QUFBQSxNQUNSLFNBQVM7QUFBQSxJQUNYO0FBQUEsRUFDRjtBQUFBLEVBQ0EsaUJBQWlCO0FBQUE7QUFBQSxJQUVmO0FBQUE7QUFBQSxJQUVBO0FBQUE7QUFBQSxJQUVBO0FBQUE7QUFBQSxJQUVBLENBQUMsUUFBUTtBQUNQLGFBQU8sSUFBSSxZQUFZLEVBQUUsU0FBUyxRQUFRO0FBQUEsSUFDNUM7QUFBQSxFQUNGO0FBQ0Y7QUFFQSxtQkFBbUIsWUFBWTtBQUFBLEVBQzdCLEdBQUc7QUFBQSxFQUNILFNBQVM7QUFBQTtBQUFBLEVBRVQ7QUFBQTtBQUFBLEVBRUEsZUFBZTtBQUFBLElBQ2IsT0FBTztBQUFBO0FBQUEsRUFDVDtBQUNGLENBQUM7QUFHRCxJQUFNLDBCQUEwQjtBQUFBO0FBQUEsRUFFOUIsa0JBQWtCO0FBQUEsRUFDbEIsV0FBVztBQUFBLEVBQ1gsaUJBQWlCO0FBQUEsRUFDakIsd0JBQXdCO0FBQUEsRUFDeEIseUJBQXlCO0FBQUEsRUFDekIseUJBQXlCO0FBQUEsRUFDekIsNkJBQTZCO0FBQUEsRUFDN0IsOEJBQThCO0FBQUEsRUFDOUIsNkJBQTZCO0FBQUEsRUFDN0Isb0NBQW9DO0FBQ3RDO0FBRUEsSUFBTyxpQkFBUSxhQUFhLFlBQVksa0JBQWtCLHVCQUF1QixDQUFDOyIsCiAgIm5hbWVzIjogW10KfQo=
