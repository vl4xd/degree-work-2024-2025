// GamePage.tsx
import React from 'react';
// import { Link } from 'react-router-dom';
import GamePredictionTable from '../components/GamePredictionTable';

const GamePage: React.FC = () => {
  return (
    <div>
      <h1>Игра</h1>
      {/* <nav style={{ display: 'flex', justifyContent: 'space-between', padding: '10px', backgroundColor: '#f0f0f0' }}>
        <Link to="/">Главная страница</Link>
        <Link to="/rpl">РПЛ</Link>
      </nav> */}
      <GamePredictionTable />
    </div>
  );
};

export default GamePage;
