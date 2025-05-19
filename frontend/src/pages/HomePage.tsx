// HomePage.tsx
import React from 'react';
import { Link } from 'react-router-dom';
import { Row, Col, Card, Typography, Divider } from 'antd';

const { Title, Paragraph } = Typography;

const HomePage: React.FC = () => {
  return (
    <div style={{ padding: '24px' }}>
      <Row justify="start">
        <Col span={24}>
          <Title level={2} style={{ textAlign: 'left' }}>Главная страница</Title>
          <Divider />
          <Paragraph style={{ textAlign: 'left', fontSize: '16px' }}>
            Добро пожаловать на нашу платформу спортивных мероприятий. Здесь вы можете найти информацию о различных спортивных событиях и следить за их прогрессом.
          </Paragraph>
        </Col>
      </Row>

      <Row justify="start" style={{ marginTop: '24px' }}>
        <Col span={24}>
          <Title level={3} style={{ textAlign: 'left' }}>Поддерживаемые спортивные мероприятия</Title>
          <Divider />
          <Row gutter={[16, 16]} justify="start">
            <Col span={24}>
              <Link to="/rpl" style={{ textDecoration: 'none' }}>
                <Card hoverable style={{ width: '100%' }}>
                  <Card.Meta
                    title="Российская Премьер Лига (РПЛ)"
                    description="Российская Премьер Лига — это высший дивизион профессионального футбола в России. Следите за расписанием, результатами и статистикой матчей."
                  />
                </Card>
              </Link>
            </Col>
          </Row>
        </Col>
      </Row>
    </div>
  );
};

export default HomePage;
