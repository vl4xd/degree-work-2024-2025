// import type { TableColumnsType } from 'antd';
// import { Badge, Space, Table } from 'antd';
import { Button, Progress } from 'antd';
import { SyncOutlined } from '@ant-design/icons';
import axios from 'axios';
import { useEffect, useState, useCallback } from 'react';


interface Prediction {
    prediction_id: number;
    min: number;
    plus_min: number;
    left_coach_id: number;
    right_coach_id: number;
    referee_id: number;
    left_num_v: number;
    left_num_z: number;
    left_num_p: number;
    left_num_n: number;
    left_num_u: number;
    right_num_v: number;
    right_num_z: number;
    right_num_p: number;
    right_num_n: number;
    right_num_u: number;
    left_num_y: number;
    left_num_y2r: number;
    right_num_y: number;
    right_num_y2r: number;
    right_num_goal_g: number;
    right_num_goal_p: number;
    right_num_goal_a: number;
    left_num_goal_g: number;
    left_num_goal_p: number;
    left_num_goal_a: number;
    left_total_transfer_value: number;
    right_total_transfer_value: number;
    left_avg_transfer_value: number;
    right_avg_transfer_value: number;
    left_goal_score: number;
    right_goal_score: number;
    left_avg_time_player_in_game: number;
    right_avg_time_player_in_game: number;
    left_right_transfer_value_div: number;
    right_left_transfer_value_div: number;
    res_event: number;
    draw_p: number;
    left_p: number;
    right_p: number;
    res_p: number;
    res: number;
    created_at: string;
    updated_at: string;
}

interface GamePrediction {
  game_id: number;
  prediction_list: Prediction[];
}

declare global {
    interface Window {
        setInterval: (callback: () => void, ms: number) => number;
        clearInterval: (id: number) => void;
    }
}

interface TeamComparison {
    num_z: number;
    num_p: number;
    num_n: number;
    avg_time: number;
    num_y: number;
    num_y2r: number;
    total_value: number;
    avg_value: number;
    value_div: number;
}

interface PredictionComparison {
    left: TeamComparison;
    right: TeamComparison;
}


function GamePredictionTable(){

    // const [data, setData] = useState<Prediction[]>([]);
    // const [loading, setLoading] = useState(true);
    // const [error, setError] = useState<string | null>(null);

    const [gamePrediction, setGamePrediction] = useState<GamePrediction>();
    const [expandedId, setExpandedId] = useState<number | null>(null);
    const [prevValues, setPrevValues] = useState<{ 
        [key: number]: PredictionComparison 
    }>({});

    const [refreshInterval, setRefreshInterval] = useState<number | null>(null); // вручную по умолчанию
    const [timeLeft, setTimeLeft] = useState<number>(100); // Процент прогресса
    const [isAuto, setIsAuto] = useState(true);
    const intervals = [
        { label: '30 сек', value: 30000 },
        { label: '1 мин', value: 60000 },
        { label: '5 мин', value: 300000 },
        { label: '10 мин', value: 600000 },
        { label: 'Вручную', value: null },
    ];
    const handleRefresh = () => {
        if (refreshInterval === null) {
            // Только для ручного режима - однократный запрос
            fetchGamePrediction();
            setIsAuto(false); // Гарантируем выключение автообновления
        } else {
            // Для автоматических режимов - переключение состояния
            setIsAuto(prev => !prev);
        }
    };
    

    const fetchGamePrediction = useCallback(() => {
        axios.get<GamePrediction>('http://127.0.0.1:8000/season/game/prediction?game_id=11077&sort_type=DESC')
            .then(response => {
                const gamePredictionResponse = response.data;
                const predictions = gamePredictionResponse.prediction_list;
                const newPrevValues: { [key: number]: PredictionComparison } = {};

                predictions.forEach((p: Prediction, index: number) => {
                    // Берем следующий (более старый) прогноз
                    const next = predictions[index + 1];
                    newPrevValues[p.prediction_id] = {
                        left: {
                            num_z: next?.left_num_z ?? p.left_num_z,
                            num_p: next?.left_num_p ?? p.left_num_p,
                            num_n: next?.left_num_n ?? p.left_num_n,
                            avg_time: next?.left_avg_time_player_in_game ?? p.left_avg_time_player_in_game,
                            num_y: next?.left_num_y ?? p.left_num_y,
                            num_y2r: next?.left_num_y2r ?? p.left_num_y2r,
                            total_value: next?.left_total_transfer_value ?? p.left_total_transfer_value,
                            avg_value: next?.left_avg_transfer_value ?? p.left_avg_transfer_value,
                            value_div: next?.left_right_transfer_value_div ?? p.left_right_transfer_value_div
                        },
                        right: {
                            num_z: next?.right_num_z ?? p.right_num_z,
                            num_p: next?.right_num_p ?? p.right_num_p,
                            num_n: next?.right_num_n ?? p.right_num_n,
                            avg_time: next?.right_avg_time_player_in_game ?? p.right_avg_time_player_in_game,
                            num_y: next?.right_num_y ?? p.right_num_y,
                            num_y2r: next?.right_num_y2r ?? p.right_num_y2r,
                            total_value: next?.right_total_transfer_value ?? p.right_total_transfer_value,
                            avg_value: next?.right_avg_transfer_value ?? p.right_avg_transfer_value,
                            value_div: next?.right_left_transfer_value_div ?? p.right_left_transfer_value_div
                        },
                    };
                });

                setPrevValues(newPrevValues);
                setGamePrediction(gamePredictionResponse);
            })
            .catch(error => {
                console.log(error);
            });
    }, []);
    

    const getChangeIndicator = (current: number, previous: number) => {
        if (current > previous) return '🟢'; // Увеличение
        if (current < previous) return '🔴'; // Уменьшение
        return '⚪'; // Без изменений
    };

    const renderProgressBar = (value: number, color: string) => {
        const percent = value * 100;
        const showInline = percent > 0; // Порог для встроенного отображения

        return (
            <div style={{
                width: `${percent}%`,
                background: color,
                display: 'flex',
                alignItems: 'center',
                justifyContent: showInline ? 'center' : 'flex-end',
                position: 'relative',
                minWidth: '50px', // Минимальная ширина для текста
                height: '32px',
                transition: 'all 0.3s ease',
            }}>
                {showInline ? (
                    <span style={{ 
                        color: 'white', 
                        padding: '0 8px',
                        whiteSpace: 'nowrap'
                    }}>
                        {percent.toFixed(1)}%
                    </span>
                ) : (
                    <span style={{
                        position: 'absolute',
                        right: '8px',
                        color: 'white',
                        fontSize: '0.8em',
                        textShadow: '0 1px 2px rgba(0,0,0,0.3)',
                        whiteSpace: 'nowrap'
                    }}>
                        {percent.toFixed(1)}%
                    </span>
                )}
            </div>
        );
    };

    useEffect(() => {
        fetchGamePrediction()
    }, [])


    // Эффект для автоматического обновления
    useEffect(() => {
        let intervalId: number;
        let timerId: number;
        let startTime = Date.now();

        if (refreshInterval !== null && isAuto) {
            intervalId = window.setInterval(() => {
                fetchGamePrediction();
                startTime = Date.now();
            }, refreshInterval);

            timerId = window.setInterval(() => {
                const passed = Date.now() - startTime;
                const percent = (passed / refreshInterval) * 100;
                setTimeLeft(percent > 100 ? 100 : percent);
            }, 1000);
        }

        return () => {
            window.clearInterval(intervalId);
            window.clearInterval(timerId);
        };
    }, [refreshInterval, isAuto, fetchGamePrediction]);

    const toggleDetails = (id: number) => {
        setExpandedId(expandedId === id ? null : id);
    };

    
    const TeamStats = ({ 
        team, 
        prevStats, 
        isLeft 
    }: { 
        team: Prediction; 
        prevStats: TeamComparison | undefined; 
        isLeft: boolean; 
    }) => {
        const fields = [
            { 
                title: 'Схема состава',
                items: [
                    { label: 'Защитники', key: 'num_z', value: isLeft ? team.left_num_z : team.right_num_z },
                    { label: 'Полузащитники', key: 'num_p', value: isLeft ? team.left_num_p : team.right_num_p },
                    { label: 'Нападающие', key: 'num_n', value: isLeft ? team.left_num_n : team.right_num_n },
                ]
            },
            {
                title: 'Время',
                items: [
                    { 
                        label: 'Ср. время на поле', 
                        key: 'avg_time', 
                        value: isLeft ? team.left_avg_time_player_in_game : team.right_avg_time_player_in_game,
                        format: (v: number) => `${Math.round(v)} мин`
                    }
                ]
            },
            {
                title: 'Наказания',
                items: [
                    { label: 'Жёлтые карточки', key: 'num_y', value: isLeft ? team.left_num_y : team.right_num_y },
                    { label: 'Вторые жёлтые', key: 'num_y2r', value: isLeft ? team.left_num_y2r : team.right_num_y2r }
                ]
            },
            {
                title: 'Стоимость',
                items: [
                    { 
                        label: 'Общая стоимость', 
                        key: 'total_value', 
                        value: isLeft ? team.left_total_transfer_value : team.right_total_transfer_value,
                        format: (v: number) => `€${(v / 1000000).toFixed(1)}M`
                    },
                    { 
                        label: 'Средняя стоимость', 
                        key: 'avg_value', 
                        value: isLeft ? team.left_avg_transfer_value : team.right_avg_transfer_value,
                        format: (v: number) => `€${(v / 1000).toFixed(1)}K`
                    },
                    { 
                        label: 'Разница в стоимости', 
                        key: 'value_div', 
                        value: isLeft ? team.left_right_transfer_value_div : team.right_left_transfer_value_div,
                        format: (v: number) => v.toFixed(2)
                    }
                ]
            }
        ];

        return (
            <div style={{ 
                background: '#f8f9fa',
                borderRadius: 8,
                padding: 16,
                boxShadow: '0 2px 8px rgba(0,0,0,0.1)'
            }}>
                <h4 style={{ 
                    color: '#1890ff',
                    borderBottom: '2px solid #1890ff',
                    paddingBottom: 8,
                    marginBottom: 16
                }}>
                    {isLeft ? 'Левая команда' : 'Правая команда'}
                </h4>
                
                {fields.map((section, i) => (
                    <div key={i} style={{ marginBottom: 24 }}>
                        <h5 style={{ 
                            color: '#595959',
                            marginBottom: 12,
                            fontSize: 14
                        }}>
                            {section.title}
                        </h5>
                        
                        {section.items.map((item, j) => {
                            const currentValue = item.value;
                            const prevValue = prevStats?.[item.key as keyof TeamComparison];
                            
                            return (
                                <div key={j} style={{ 
                                    display: 'flex',
                                    justifyContent: 'space-between',
                                    alignItems: 'center',
                                    marginBottom: 8,
                                    padding: 8,
                                    background: j % 2 === 0 ? '#fff' : '#f5f5f5',
                                    borderRadius: 4
                                }}>
                                    <span style={{ color: '#8c8c8c' }}>{item.label}</span>
                                    <div style={{ display: 'flex', alignItems: 'center' }}>
                                        {prevValue !== undefined && (
                                            <>
                                                {getChangeIndicator(currentValue, prevValue)}
                                                <span style={{ marginLeft: 4 }}>
                                                    ({prevValue.toFixed(1)} → {currentValue.toFixed(1)})
                                                </span>
                                            </>
                                        )}
                                        <span style={{ 
                                            fontWeight: 500,
                                            minWidth: 80,
                                            textAlign: 'right'
                                        }}>
                                            {/* {item.format 
                                                ? item.format(currentValue)
                                                : currentValue} */}
                                        </span>
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                ))}
            </div>
        );
    };

    // const StatItem = ({ label, value }: { label: string; value: React.ReactNode }) => (
    //     <div style={{ 
    //         display: 'flex',
    //         justifyContent: 'space-between',
    //         alignItems: 'center',
    //         padding: 8,
    //         background: '#fff',
    //         borderRadius: 4
    //     }}>
    //         <span style={{ color: '#8c8c8c' }}>{label}</span>
    //         <span style={{ 
    //             fontWeight: 500,
    //             color: '#262626',
    //             maxWidth: '60%',
    //             textAlign: 'right'
    //         }}>
    //             {value}
    //         </span>
    //     </div>
    // );

    return (
        <div style={{ margin: '0 auto'}}>

            {/* Верхняя панель управления */}
            <div style={{ 
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                flexWrap: 'wrap',
                gap: 16,
                marginBottom: 16
            }}>
                {/* Блок выбора интервала слева */}
                <div style={{ 
                    display: 'flex',
                    flexWrap: 'wrap',
                    gap: 8,
                    justifyContent: 'flex-start'
                }}>
                    {intervals.map(({ label, value }) => (
                        <Button
                            key={label}
                            type={refreshInterval === value ? 'primary' : 'default'}
                            size="small"
                            onClick={() => {
                                setRefreshInterval(value);
                                setIsAuto(value !== null);
                            }}
                        >
                            {label}
                        </Button>
                    ))}
                </div>

                {/* Кнопка обновления справа */}
                <Button 
                    type="primary"
                    onClick={handleRefresh}
                    icon={<SyncOutlined spin={isAuto && refreshInterval !== null} />}
                >
                    {refreshInterval === null 
                        ? 'Обновить сейчас' 
                        : isAuto 
                            ? 'Остановить автообновление' 
                            : 'Запустить автообновление'}
                </Button>
            </div>

            {/* Прогресс-бар */}
            {refreshInterval !== null && isAuto && (
                <Progress
                    percent={timeLeft}
                    status="active"
                    showInfo={false}
                    style={{ 
                        marginBottom: 24,
                        width: '100%'
                    }}
                />
            )}

            {gamePrediction?.prediction_list.map(prediction => (
                <div 
                    key={prediction.prediction_id}
                    style={{ 
                        marginBottom: 8,
                        border: '1px solid #e8e8e8',
                        borderRadius: 4,
                        overflow: 'hidden'
                    }}
                >
                    <div 
                        style={{ 
                            padding: 16,
                            background: '#fafafa',
                            cursor: 'pointer',
                            display: 'flex',
                            alignItems: 'center'
                        }}
                        onClick={() => toggleDetails(prediction.prediction_id)}
                    >
                        <div style={{ width: 80 }}>
                            {prediction.min}'{prediction.plus_min > 0 && `+${prediction.plus_min}`}
                        </div>
                        
                        <div style={{ 
                            flexGrow: 1,
                            height: 32,
                            borderRadius: 4,
                            overflow: 'hidden',
                            display: 'flex'
                        }}>
                            {renderProgressBar(prediction.left_p, '#f50')}
                            {renderProgressBar(prediction.draw_p, '#2db7f5')}
                            {renderProgressBar(prediction.right_p, '#87d068')}
                        </div>
                    </div>


                    {/* Детали прогноза */}
                    {expandedId === prediction.prediction_id && (
                        <div style={{ 
                            padding: 24,
                            background: '#fff',
                            borderTop: '1px solid #f0f0f0'
                        }}>
                            <div style={{ 
                                display: 'grid',
                                gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))',
                                gap: 24,
                                alignItems: 'start'
                            }}>
                                {/* Статистика левой команды */}
                                <TeamStats 
                                    team={prediction} 
                                    prevStats={prevValues[prediction.prediction_id]?.left}
                                    isLeft={true}
                                    // teamColor="#ff4d4f"
                                />

                                {/* Общая статистика матча */}
                                {/* <div style={{ 
                                    background: '#f8f9fa',
                                    borderRadius: 8,
                                    padding: 16,
                                    boxShadow: '0 2px 8px rgba(0,0,0,0.05)'
                                }}>
                                    <h4 style={{
                                        color: '#1890ff',
                                        marginBottom: 16,
                                        paddingBottom: 8,
                                        borderBottom: '2px solid #1890ff'
                                    }}>
                                        Общая статистика
                                    </h4>
                                    
                                    <div style={{ 
                                        display: 'grid',
                                        gap: 12 
                                    }}>
                                        <StatItem 
                                            label="Судья"
                                            value={`ID: ${prediction.referee_id}`}
                                        />
                                        <StatItem 
                                            label="Результат"
                                            value={
                                                prediction.res === 0 
                                                    ? 'Ничья' 
                                                    : `Победа ${prediction.res === 1 ? 'левых' : 'правых'}`
                                            }
                                        />
                                        <StatItem 
                                            label="Вероятность исхода"
                                            value={
                                                <div style={{ 
                                                    display: 'flex', 
                                                    gap: 8,
                                                    flexWrap: 'wrap'
                                                }}>
                                                    <Tag color="#ff4d4f">Левые: {(prediction.left_p * 100).toFixed(1)}%</Tag>
                                                    <Tag color="#1890ff">Ничья: {(prediction.draw_p * 100).toFixed(1)}%</Tag>
                                                    <Tag color="#52c41a">Правые: {(prediction.right_p * 100).toFixed(1)}%</Tag>
                                                </div>
                                            }
                                        />
                                    </div>
                                </div> */}

                                {/* Статистика правой команды */}
                                <TeamStats 
                                    team={prediction} 
                                    prevStats={prevValues[prediction.prediction_id]?.right}
                                    isLeft={false}
                                    // teamColor="#52c41a"
                                />
                            </div>
                        </div>
                    )}
                </div>
            ))}
        </div>
    );
}

export default GamePredictionTable