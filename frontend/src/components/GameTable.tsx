import React, { useState, useEffect, useCallback } from "react";
import axios from 'axios';
import { Table, Select, DatePicker, Button, Space, Alert } from 'antd';
// import moment from 'moment';
import { Dayjs } from 'dayjs';
import { useNavigate } from 'react-router-dom';

const { Option } = Select;
const { RangePicker } = DatePicker;

interface Season {
    season_id: string;
    start_date: string;
    end_date: string;
    season_url: string;
}

interface SeasonTeam {
    season_id: string;
    team_id: string;
    season_team_id: string;
    name: string;
    season_team_url: string;
}

interface Game {
    game_id: number;
    season_game_id: string;
    season_id: string;
    left_team_id: string;
    right_team_id: string;
    game_status_id: number;
    left_coach_id: string;
    right_coach_id: string;
    tour_number: number;
    start_date: string;
    start_time: string;
    min: number | null;
    plus_min: number | null;
    created_at: string;
    updated_at: string;
    game_url: string;
}

const GameTable: React.FC = () => {
    const navigate = useNavigate();
    const [seasons, setSeasons] = useState<Season[]>([]);
    const [seasonTeams, setSeasonTeams] = useState<SeasonTeam[]>([]);
    const [games, setGames] = useState<Game[]>([]);
    const [selectedSeason, setSelectedSeason] = useState<string>('');
    const [selectedTeams, setSelectedTeams] = useState<{ leftTeamId: string; rightTeamId: string }>({ leftTeamId: '', rightTeamId: '' });
    // const [dateRange, setDateRange] = useState<[moment.Moment | null, moment.Moment | null]>([null, null]);
    const [dateRange, setDateRange] = useState<[Dayjs | null, Dayjs | null]>([null, null]);
    const [gameStatuses, setGameStatuses] = useState<number[]>([0, 2, 3]);
    const [loading, setLoading] = useState({
        seasons: true,
        teams: false,
        games: false
    });
    const [error, setError] = useState<string>('');
    const [pagination, setPagination] = useState({ limit: 8, page: 0 });
    const [hasMore, setHasMore] = useState(false);

    const fetchSeasons = useCallback(() => {
        axios.get<Season[]>('http://localhost:8000/seasons')
            .then(response => {
                const seasonsResponse = response.data;
                setSeasons(seasonsResponse);
                setSelectedSeason(seasonsResponse[seasonsResponse.length - 1]?.season_id || '');
            })
            .catch(err => setError(err.message))
            .finally(() => setLoading(prev => ({ ...prev, seasons: false })));
    }, []);

    const fetchSeasonTeams = useCallback(() => {
        if (!selectedSeason) return;

        axios.get<SeasonTeam[]>(`http://localhost:8000/season/teams?season_id=${selectedSeason}`)
            .then(response => {
                setSeasonTeams(response.data);
            })
            .catch(err => setError(err.message))
            .finally(() => setLoading(prev => ({ ...prev, teams: false })));
    }, [selectedSeason]);

    const fetchSeasonGames = useCallback(() => {
        if (!selectedSeason) return;

        setLoading(prev => ({ ...prev, games: true }));

        let queryString = `sort_type=ASC&season_id=${selectedSeason}&limit=${pagination.limit}&offset=${pagination.page * pagination.limit}`;

        gameStatuses.forEach((status) => {
            queryString += `&game_statuses=${status}`;
        });

        if (selectedTeams.leftTeamId) queryString += `&left_team_id=${selectedTeams.leftTeamId}`;
        if (selectedTeams.rightTeamId) queryString += `&right_team_id=${selectedTeams.rightTeamId}`;
        if (dateRange[0]) queryString += `&from_start_date=${dateRange[0].format('YYYY-MM-DD')}`;
        if (dateRange[1]) queryString += `&to_start_date=${dateRange[1].format('YYYY-MM-DD')}`;

        axios.get<Game[]>(`http://localhost:8000/season/games?${queryString}`)
            .then(response => {
                setGames(response.data);
                setHasMore(response.data.length >= pagination.limit);
            })
            .catch(err => setError(err.message))
            .finally(() => setLoading(prev => ({ ...prev, games: false })));
    }, [selectedSeason, selectedTeams, dateRange, gameStatuses, pagination]);

    useEffect(() => {
        fetchSeasons();
    }, [fetchSeasons]);

    useEffect(() => {
        fetchSeasonTeams();
        setSelectedTeams({ leftTeamId: '', rightTeamId: '' });
    }, [selectedSeason, fetchSeasonTeams]);

    useEffect(() => {
        fetchSeasonGames();
    }, [fetchSeasonGames]);

    useEffect(() => {
        setPagination(prev => ({ ...prev, page: 0 }));
    }, [selectedSeason, selectedTeams, dateRange, gameStatuses]);

    const handlePrevious = () => {
        setPagination(prev => ({ ...prev, page: Math.max(prev.page - 1, 0) }));
    };

    const handleNext = () => {
        setPagination(prev => ({ ...prev, page: prev.page + 1 }));
    };

    const handleLimitChange = (value: number) => {
        setPagination({ limit: value, page: 0 });
    };

    const clearLeftTeam = () => {
        setSelectedTeams(prev => ({ ...prev, leftTeamId: '' }));
    };

    const clearRightTeam = () => {
        setSelectedTeams(prev => ({ ...prev, rightTeamId: '' }));
    };

    const clearDateRange = () => {
        setDateRange([null, null]);
    };

    const clearGameStatuses = () => {
        setGameStatuses([]);
    };

    const getGameStatusName = (statusId: number) => {
        switch (statusId) {
            case 0: return 'Не начата';
            case 1: return 'Завершена';
            case 2: return 'Перерыв';
            case 3: return 'В процессе';
            default: return 'Неизвестно';
        }
    };

    const columns = [
        { title: 'Season Game ID', dataIndex: 'season_game_id', key: 'season_game_id' },
        { title: 'Left Team ID', dataIndex: 'left_team_id', key: 'left_team_id' },
        { title: 'Right Team ID', dataIndex: 'right_team_id', key: 'right_team_id' },
        { title: 'Start Date', dataIndex: 'start_date', key: 'start_date' },
        { title: 'Start Time', dataIndex: 'start_time', key: 'start_time' },
        {
            title: 'Game Status',
            dataIndex: 'game_status_id',
            key: 'game_status_id',
            render: (statusId: number) => getGameStatusName(statusId)
        },
    ];

    return (
        <Space direction="vertical" size="middle" style={{ display: 'flex', width: '100%' }}>
            {error && <Alert message={error} type="error" showIcon closable onClose={() => setError('')} />}

            <Select
                placeholder="Выберите сезон"
                value={selectedSeason}
                onChange={setSelectedSeason}
                style={{ width: '100%' }}
            >
                {seasons.map(season => (
                    <Option key={season.season_id} value={season.season_id}>
                        {`${season.start_date} - ${season.end_date}`}
                    </Option>
                ))}
            </Select>

            <div style={{ display: 'flex', alignItems: 'center' }}>
                <Select
                    placeholder="Выберите левую команду"
                    value={selectedTeams.leftTeamId}
                    onChange={value => setSelectedTeams(prev => ({ ...prev, leftTeamId: value }))}
                    style={{ flex: 1 }}
                >
                    {seasonTeams.map(team => (
                        <Option key={team.team_id} value={team.team_id}>
                            {team.name}
                        </Option>
                    ))}
                </Select>
                <Button onClick={clearLeftTeam} style={{ marginLeft: 8 }}>Очистить</Button>
            </div>

            <div style={{ display: 'flex', alignItems: 'center' }}>
                <Select
                    placeholder="Выберите правую команду"
                    value={selectedTeams.rightTeamId}
                    onChange={value => setSelectedTeams(prev => ({ ...prev, rightTeamId: value }))}
                    style={{ flex: 1 }}
                >
                    {seasonTeams.map(team => (
                        <Option key={team.team_id} value={team.team_id}>
                            {team.name}
                        </Option>
                    ))}
                </Select>
                <Button onClick={clearRightTeam} style={{ marginLeft: 8 }}>Очистить</Button>
            </div>

            <div style={{ display: 'flex', alignItems: 'center' }}>
                <RangePicker
                    value={dateRange}
                    // onChange={dates => setDateRange(dates as [moment.Moment | null, moment.Moment | null])}
                    onChange={(dates) => setDateRange(dates as [Dayjs | null, Dayjs | null])}
                    allowClear={false}
                    style={{ flex: 1 }}
                />
                <Button onClick={clearDateRange} style={{ marginLeft: 8 }}>Очистить</Button>
            </div>

            <div style={{ display: 'flex', alignItems: 'center' }}>
                <Select
                    mode="multiple"
                    placeholder="Выберите статусы игр"
                    value={gameStatuses}
                    onChange={setGameStatuses}
                    style={{ flex: 1 }}
                >
                    <Option value={0}>Не начата</Option>
                    <Option value={1}>Завершена</Option>
                    <Option value={2}>Перерыв</Option>
                    <Option value={3}>В процессе</Option>
                </Select>
                <Button onClick={clearGameStatuses} style={{ marginLeft: 8 }}>Очистить</Button>
            </div>

            <Table
                columns={columns}
                dataSource={games}
                rowKey="season_game_id"
                loading={loading.games}
                style={{ width: '100%' }}
                pagination={false}
                onRow={(record) => ({
                    onClick: () => {
                        navigate(`/rpl/game?game_id=${record.game_id}`);
                    },
                })}
            />

            <div style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                marginTop: 16
            }}>
                <Button
                    onClick={handlePrevious}
                    disabled={pagination.page === 0}
                    style={{ flex: 1, margin: '0 8px' }}
                >
                    Назад
                </Button>

                <Select
                    value={pagination.limit}
                    onChange={handleLimitChange}
                    style={{ width: 120 }}
                >
                    <Option value={8}>8 записей</Option>
                    <Option value={30}>30 записей</Option>
                    <Option value={80}>80 записей</Option>
                </Select>

                <Button
                    onClick={handleNext}
                    disabled={!hasMore}
                    style={{ flex: 1, margin: '0 8px' }}
                >
                    Вперед
                </Button>
            </div>
        </Space>
    );
};

export default GameTable;
