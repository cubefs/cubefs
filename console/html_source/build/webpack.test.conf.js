'use strict'
const path = require('path')
const utils = require('./utils')
const webpack = require('webpack')
const config = require('../config')
const merge = require('webpack-merge')
const baseWebpackConfig = require('./webpack.base.conf')
// const CopyWebpackPlugin = require('copy-webpack-plugin') // 用于将static中的静态文件复制到产品文件夹dist
const HtmlWebpackPlugin = require('html-webpack-plugin')
const ExtractTextPlugin = require('extract-text-webpack-plugin') // 抽离css样式,防止将样式打包在js中引起页面样式加载错乱的现象;
const OptimizeCSSPlugin = require('optimize-css-assets-webpack-plugin') // 用于优化和最小化css资源
const UglifyJsPlugin = require('uglifyjs-webpack-plugin') // 压缩代码

const env = require('../config/test.env')

const webpackConfig = merge(baseWebpackConfig, {
	module: {
		/**
		 * 样式文件的处理规则，对css/sass/scss等不同内容使用相应的styleLoaders
		 * 由utils配置出各种类型的预处理语言所需要使用的loader，例如sass需要使用sass-loader
		 */
		rules: utils.styleLoaders({
			sourceMap: config.test.productionSourceMap,
			extract: true,
			usePostCSS: true
		})
	},

	// 是否使用source-map
	devtool: config.test.productionSourceMap ? config.test.devtool : false,

	// webpack输出路径和命名规则
	output: {
		path: config.test.assetsRoot,
		filename: utils.assetsPath('js/[name].[chunkhash].js'),
		chunkFilename: utils.assetsPath('js/[id].[chunkhash].js')
	},

	// webpack插件
	plugins: [
		new webpack.DefinePlugin({
			'process.env': env
		}),

		// 丑化压缩JS代码
		new UglifyJsPlugin({
			uglifyOptions: {
				compress: {
					warnings: false
				}
			},
			sourceMap: config.test.productionSourceMap,
			parallel: true
		}),

		// 将css提取到单独的文件
		new ExtractTextPlugin({
			filename: utils.assetsPath('css/[name].[contenthash].css'),
			// Setting the following option to `false` will not extract CSS from codesplit chunks.
			// Their CSS will instead be inserted dynamically with style-loader when the codesplit chunk has been loaded by webpack.
			// It's currently set to `true` because we are seeing that sourcemaps are included in the codesplit bundle as well when it's `false`,
			// increasing file size: https://github.com/vuejs-templates/webpack/issues/1110
			allChunks: true,
		}),

		/**
		 * 优化、最小化css代码，如果只简单使用extract-text-plugin可能会造成css重复
		 * 具体原因可以看npm上面optimize-css-assets-webpack-plugin的介绍
		 */
		new OptimizeCSSPlugin({
			cssProcessorOptions: config.test.productionSourceMap ?
				{
					safe: true,
					map: {
						inline: false
					}
				} :
				{
					safe: true
				}
		}),

		// 将产品文件的引用注入到index.html
		new HtmlWebpackPlugin({
			filename: config.test.index,
			template: 'index.html',
			inject: true,
			minify: {
				removeComments: true, // 删除index.html中的注释
				collapseWhitespace: true, // 删除index.html中的空格
				removeAttributeQuotes: true // 删除各种html标签属性值的双引号
			},

			// 注入依赖的时候按照依赖先后顺序进行注入，比如，需要先注入vendor.js，再注入app.js
			chunksSortMode: 'dependency'
		}),

		// 保持模块ID不变。当vender模块不变时不更新该模块，利于缓存
		new webpack.HashedModuleIdsPlugin(),

		new webpack.optimize.ModuleConcatenationPlugin(),

		// 将所有从node_modules中引入的js提取到vendor.js，即抽取库文件
		new webpack.optimize.CommonsChunkPlugin({
			name: 'vendor',
			minChunks(module) {
				// any required modules inside node_modules are extracted to vendor
				return (
					module.resource &&
					/\.js$/.test(module.resource) &&
					module.resource.indexOf(
						path.join(__dirname, '../node_modules')
					) === 0
				)
			}
		}),

		new webpack.optimize.CommonsChunkPlugin({
			name: 'manifest',
			minChunks: Infinity
		}),
		// This instance extracts shared chunks from code splitted chunks and bundles them
		// in a separate chunk, similar to the vendor chunk
		// see: https://webpack.js.org/plugins/commons-chunk-plugin/#extra-async-commons-chunk
		new webpack.optimize.CommonsChunkPlugin({
			name: 'app',
			async: 'vendor-async',
			children: true,
			minChunks: 3
		}),

		// 将static文件夹里面的静态资源复制到dist/static
		// new CopyWebpackPlugin([{
		// 	from: path.resolve(__dirname, '../static'),
		// 	to: config.test.assetsSubDirectory,
		// 	ignore: ['.*']
		// }]),

	]
})

// 如果开启了产品gzip压缩，则利用插件将构建后的产品文件进行压缩
if (config.test.productionGzip) {
	const CompressionWebpackPlugin = require('compression-webpack-plugin')

	webpackConfig.plugins.push(
		new CompressionWebpackPlugin({
			asset: '[path].gz[query]',
			algorithm: 'gzip',
			test: new RegExp(
				'\\.(' +
				config.test.productionGzipExtensions.join('|') +
				')$'
			),
			threshold: 10240,
			minRatio: 0.8
		})
	)
}

// 如果启动了report，则通过插件给出webpack构建打包后的产品文件分析报告
if (config.test.bundleAnalyzerReport) {
	const BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin
	webpackConfig.plugins.push(new BundleAnalyzerPlugin())
}

module.exports = webpackConfig
