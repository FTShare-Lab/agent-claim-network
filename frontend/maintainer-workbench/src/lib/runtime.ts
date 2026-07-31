// 公开 Pages 构建只展示合成数据，不连接 Maintainer 或 Router 服务。
export const isStaticDemo = import.meta.env.MODE === 'pages'
