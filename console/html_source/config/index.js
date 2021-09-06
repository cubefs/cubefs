'use strict'
// Template version: 1.3.1
// see http://vuejs-templates.github.io/webpack for documentation.

const path = require('path')

module.exports = {
  dev: {

    // Paths
    assetsSubDirectory: 'static',
    assetsPublicPath: '/',
    proxyTable: {
      '/proxy': {
        target: 'http://console.chubao.io/', // 测试环境API
        changeOrigin: true,
        pathRewrite: {
          '^/proxy': '/'
        }
      },
      '/api': {
        target: 'http://console.chubao.io/', // 测试环境API
        changeOrigin: true,
        pathRewrite: {
          '^/api': '/api'
        }
      }
    },

    // Various Dev Server settings
    host: 'dev.jd.com', // can be overwritten by process.env.HOST
    port: 8080, // can be overwritten by process.env.PORT, if port is in use, a free one will be determined
    autoOpenBrowser: false,
    errorOverlay: true,
    notifyOnErrors: true,
    poll: false, // https://webpack.js.org/configuration/dev-server/#devserver-watchoptions-


    /**
     * Source Maps
     */

    // https://webpack.js.org/configuration/devtool/#development
    devtool: 'cheap-module-eval-source-map',

    // If you have problems debugging vue-files in devtools,
    // set this to false - it *may* help
    // https://vue-loader.vuejs.org/en/options.html#cachebusting
    cacheBusting: true,

    cssSourceMap: true
  },

  build: {
    // Template for index.html
    index: path.resolve(__dirname, '../dist/index.html'),

    // Paths
    assetsRoot: path.resolve(__dirname, '../dist'),
    assetsSubDirectory: 'static',
    assetsPublicPath: '/',

    /**
     * Source Maps
     */

    productionSourceMap: true,
    // https://webpack.js.org/configuration/devtool/#production
    devtool: '#source-map',

    // Gzip off by default as many popular static hosts such as
    // Surge or Netlify already gzip all static assets for you.
    // Before setting to `true`, make sure to:
    // npm install --save-dev compression-webpack-plugin
    productionGzip: false,
    productionGzipExtensions: ['js', 'css'],

    // Run the build command with an extra argument to
    // View the bundle analyzer report after build finishes:
    // `npm run build --report`
    // Set to `true` or `false` to always turn it on or off
    bundleAnalyzerReport: process.env.npm_config_report
  },

  // 测试环境配置数据
  test: {
    index: path.resolve(__dirname, '../server/views/index.html'),

    // 生产环境的路径配置
    assetsRoot: path.resolve(__dirname, '../server'), // 配置打包时生成的文件的根目录
    assetsSubDirectory: 'static', // 二级目录，存放静态资源文件的目录，位于static文件夹下
    assetsPublicPath: '/', // 测试环境的发布路径

    indexHtmlPublicPath: '', // 修改注入到index.html的链接的前缀，为空则不修改

    productionSourceMap: false, // 生产环境打包的时候是否开启SourceMap
    devtool: '#source-map', // 如果productionSourceMap为true，有效

    productionGzip: false, // 是否开启Gzip压缩
    productionGzipExtensions: ['js', 'css'], // gzip模式下需要压缩的文件的扩展名，当前只会对js和css文件进行压缩

    bundleAnalyzerReport: process.env.npm_config_report // 是否展示webpack构建打包之后的分析报告
  }
}
