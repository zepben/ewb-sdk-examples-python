/*
 * Copyright 2021 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

const zepbenDocusaurusPreset = require("@zepben/docusaurus-preset");
const versions = require("./versions.json");

module.exports = {
  title: "Python SDK Examples",
  tagline: "",
  url: "https://zepben.github.io",
  baseUrl: "/evolve/docs/ewb-sdk-examples-python/",
  onBrokenLinks: "throw",
  favicon: "img/favicon.ico",
  organizationName: "zepben",
  projectName: "ewb-sdk-examples-python",
  themeConfig: {
    colorMode: {
      defaultMode: "light",
      disableSwitch: false,
      respectPrefersColorScheme: true,
    },
    navbar: {
      logo: {
        alt: "Zepben",
        src: "img/logo.svg",
        srcDark: "img/logo-dark.svg",
        href: "https://www.zepben.com/",
      },
      items: [
        {
          to: "https://zepben.github.io/evolve/docs",
          label: "Evolve",
          position: "left",
        },
        {
          to: "/",
          activeBasePath: "/",
          label: "Docs",
          position: "left",
        },
        {
          to: "release-notes",
          activeBasePath: "release-notes",
          label: "Release Notes",
          position: "right",
        },
        {
          type: "docsVersionDropdown",
          position: "right",
        },
        {
          href: "https://github.com/zepben/ewb-sdk-examples-python",
          position: 'right',
          className: 'header-github-link',
          'aria-label': 'GitHub repository',
        },
      ],
    },
    footer: {
      style: "dark",
      links: [],
      copyright: `Copyright © ${new Date().getFullYear()} Zeppelin Bend Pty. Ltd.`,
    },
    algolia: {
      ...zepbenDocusaurusPreset.defaultThemeConfig.algolia,
      searchParameters: {
        facetFilters: ["project:ewb-sdk-examples-python"]
      }
    },
    metadata: [{name: 'docsearch:project', content: 'ewb-sdk-examples-python'}],
  },
  presets: [
    [
      "@zepben/docusaurus-preset",
      {
        gtag: { ...zepbenDocusaurusPreset.defaultThemeConfig.gtag },
        docs: {
          routeBasePath: '/',
          sidebarPath: require.resolve("./sidebars.js"),
          versions: versions.reduce((acc, curr) => {
            acc[curr] = {label: curr, path: curr};
            return acc;
          }, {})
        },
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
      },
    ],
  ],
};
