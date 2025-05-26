// App.tsx
import React from 'react';
import { BrowserRouter as Router, Route, Routes, Link, useLocation } from 'react-router-dom';
import { Layout, Breadcrumb } from 'antd';
import HomePage from './pages/HomePage';
import RPLPage from './pages/RPLPage';
import GamePage from './pages/GamePage';

const { Header, Content, Footer } = Layout;

const AppHeader: React.FC = () => {
  const location = useLocation();

  // Определяем путь для отображения в хлебных крошках
  const getBreadcrumbItems = () => {
    const path = location.pathname;
    if (path === '/') {
      return [<Breadcrumb.Item key="home">Главная страница</Breadcrumb.Item>];
    } else if (path === '/rpl') {
      return [
        <Breadcrumb.Item key="home"><Link to="/">Главная страница</Link></Breadcrumb.Item>,
        <Breadcrumb.Item key="rpl">РПЛ</Breadcrumb.Item>
      ];
    } else if (path === '/rpl/game') {
      return [
        <Breadcrumb.Item key="home"><Link to="/">Главная страница</Link></Breadcrumb.Item>,
        <Breadcrumb.Item key="rpl"><Link to="/rpl">РПЛ</Link></Breadcrumb.Item>,
        <Breadcrumb.Item key="game">Игра</Breadcrumb.Item>
      ];
    }
    return [<Breadcrumb.Item key="home">Главная страница</Breadcrumb.Item>];
  };

  return (
    <Header style={{ display: 'flex', alignItems: 'center', background: '#fff' }}>
      {/* <Menu mode="horizontal" defaultSelectedKeys={['1']} style={{ flex: 1, minWidth: 0 }}>
        <Menu.Item key="1">
          <Link to="/">Главная страница</Link>
        </Menu.Item>
        <Menu.Item key="2">
          <Link to="/rpl">РПЛ</Link>
        </Menu.Item>
      </Menu> */}
      <Breadcrumb style={{ marginLeft: 'auto', flex: 1 }}>
        {getBreadcrumbItems()}
      </Breadcrumb>
    </Header>
  );
};

const App: React.FC = () => {
  return (
    <Router>
      <Layout style={{ minHeight: '100vh' }}>
        <AppHeader />
        <Content style={{ padding: '0 50px' }}>
          <div style={{ background: '#fff', padding: 24, minHeight: 380 }}>
            <Routes>
              <Route path="/" element={<HomePage />} />
              <Route path="/rpl" element={<RPLPage />} />
              <Route path="/rpl/game" element={<GamePage />} />
            </Routes>
          </div>
        </Content>
        <Footer style={{ textAlign: 'center' }}>
          Выпускная квалификационная работа ©2025 Платонов Владислав Алексеевич
        </Footer>
      </Layout>
    </Router>
  );
};

export default App;
