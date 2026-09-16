// 页面测试的会话元数据夹具；实际 Cookie 生命周期另由鉴权与浏览器测试覆盖。
export function seedAdminSession(id = 'test-session') {
  window.localStorage.setItem('acn-maintainer-admin-session-v2', JSON.stringify({
    revision: id,
    session: { id, username: 'admin', expires_at: 4102444800 },
  }))
}
