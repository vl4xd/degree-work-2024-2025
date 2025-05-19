// RPLPage.tsx
import React from 'react';
// import { Link } from 'react-router-dom';
import GameTable from '../components/GameTable';

const RPLPage: React.FC = () => {
  return (
    <div>
      <h1>РПЛ</h1>
      {/* <nav style={{ display: 'flex', justifyContent: 'space-between', padding: '10px', backgroundColor: '#f0f0f0' }}>
        <Link to="/">Главная страница</Link>
        <Link to="/rpl/game">Игра</Link>
      </nav> */}
      <GameTable />
    </div>
  );
};

export default RPLPage;
