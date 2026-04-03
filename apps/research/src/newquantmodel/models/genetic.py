from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Callable

import numpy as np
import pandas as pd


@dataclass(slots=True)
class GAConfig:
    population: int = 32
    generations: int = 50
    elitism_ratio: float = 0.10
    tournament_size: int = 3
    mutation_rate: float = 0.08
    mutation_scale: float = 0.15
    seed: int = 7
    patience: int = 8


@dataclass(slots=True)
class GAResult:
    chromosome: np.ndarray
    fitness: float
    metrics: dict[str, float]
    payload: dict[str, object]
    generations_run: int


def _portfolio_metrics(returns: pd.Series, periods_per_year: int) -> tuple[float, float, float]:
    if returns.empty:
        return 0.0, 0.0, 0.0
    equity = (1.0 + returns.fillna(0.0)).cumprod()
    periods = len(returns)
    cagr = equity.iloc[-1] ** (periods_per_year / max(periods, 1)) - 1.0
    std = float(returns.std() or 0.0)
    sharpe = 0.0 if abs(std) < 1e-12 else float(returns.mean() / std * math.sqrt(periods_per_year))
    drawdown = equity / equity.cummax() - 1.0
    return float(cagr), sharpe, float(drawdown.min())


def _safe_corr(left: pd.Series, right: pd.Series) -> float | None:
    if left.nunique(dropna=True) <= 1 or right.nunique(dropna=True) <= 1:
        return None
    return float(left.corr(right, method="pearson"))


def score_candidate_history(
    history: pd.DataFrame,
    *,
    market: str,
    target_col: str,
    score_col: str = "score",
    top_n: int = 10,
    feature_count: int = 0,
    total_features: int = 1,
) -> tuple[float, dict[str, float]]:
    if history.empty or target_col not in history.columns or score_col not in history.columns:
        metrics = {
            "fitness": -999.0,
            "sharpe": 0.0,
            "cagr": 0.0,
            "maxDrawdown": 0.0,
            "hitRate": 0.0,
            "ic": 0.0,
            "rankIc": 0.0,
            "topDecileSpread": 0.0,
            "turnover": 0.0,
            "stabilityPenalty": 1.0,
            "featureCountPenalty": float(feature_count / max(total_features, 1)),
        }
        return metrics["fitness"], metrics

    periods_per_year = 252 if market != "weekly" else 52
    period_returns: list[float] = []
    hit_rates: list[float] = []
    spreads: list[float] = []
    ics: list[float] = []
    rank_ics: list[float] = []
    turnover = 0.0
    previous_weights: dict[str, float] = {}
    step_returns: list[float] = []

    for _, day in history.groupby("timestamp", sort=True):
        day = day.dropna(subset=[score_col, target_col]).sort_values(score_col, ascending=False).copy()
        if day.empty:
            continue
        score_rank = day[score_col].rank(ascending=False, method="average")
        ic_value = _safe_corr(day[score_col], day[target_col])
        rank_ic_value = _safe_corr(score_rank, day[target_col].rank(ascending=False, method="average"))
        if ic_value is not None:
            ics.append(ic_value)
        if rank_ic_value is not None:
            rank_ics.append(rank_ic_value)

        take = max(1, min(top_n, len(day)))
        longs = day.head(take)
        shorts = day.tail(take)
        portfolio_return = float(longs[target_col].mean())
        spread = portfolio_return - float(shorts[target_col].mean()) if not shorts.empty else portfolio_return
        weights = {str(symbol): 1.0 / take for symbol in longs["symbol"].astype(str)}
        overlap_keys = set(previous_weights) | set(weights)
        turnover_step = sum(abs(weights.get(key, 0.0) - previous_weights.get(key, 0.0)) for key in overlap_keys) / 2.0
        previous_weights = weights
        turnover += turnover_step
        period_returns.append(portfolio_return)
        hit_rates.append(1.0 if portfolio_return > 0.0 else 0.0)
        spreads.append(spread)
        step_returns.append(portfolio_return)

    returns = pd.Series(period_returns, dtype="float64")
    cagr, sharpe, max_drawdown = _portfolio_metrics(returns, periods_per_year)
    turnover_mean = turnover / max(len(period_returns), 1)
    stability_penalty = float(pd.Series(step_returns, dtype="float64").rolling(12, min_periods=4).std().mean() or 0.0)
    feature_penalty = float(feature_count / max(total_features, 1))
    metrics = {
        "sharpe": sharpe,
        "cagr": cagr,
        "maxDrawdown": max_drawdown,
        "hitRate": float(np.nanmean(hit_rates) if hit_rates else 0.0),
        "ic": float(np.nanmean(ics) if ics else 0.0),
        "rankIc": float(np.nanmean(rank_ics) if rank_ics else 0.0),
        "topDecileSpread": float(np.nanmean(spreads) if spreads else 0.0),
        "turnover": turnover_mean,
        "stabilityPenalty": stability_penalty,
        "featureCountPenalty": feature_penalty,
    }
    fitness = (
        0.35 * metrics["sharpe"]
        + 0.20 * max(metrics["rankIc"], metrics["ic"])
        + 0.20 * metrics["topDecileSpread"]
        + 0.15 * metrics["hitRate"]
        - 0.15 * metrics["turnover"]
        - 0.10 * abs(min(metrics["maxDrawdown"], 0.0))
        - 0.05 * metrics["featureCountPenalty"]
        - 0.05 * metrics["stabilityPenalty"]
    )
    metrics["fitness"] = float(fitness)
    return float(fitness), metrics


def run_genetic_search(
    *,
    dimensions: int,
    evaluator: Callable[[np.ndarray], tuple[float, dict[str, float], dict[str, object]]],
    config: GAConfig | None = None,
) -> GAResult:
    cfg = config or GAConfig()
    rng = np.random.default_rng(cfg.seed)
    population = rng.random((cfg.population, dimensions), dtype=np.float64)
    elite_count = max(1, int(round(cfg.population * cfg.elitism_ratio)))
    best_fitness = -float("inf")
    best_chromosome = population[0].copy()
    best_metrics: dict[str, float] = {}
    best_payload: dict[str, object] = {}
    stale_generations = 0
    generations_run = 0

    def _evaluate_population(pop: np.ndarray) -> tuple[np.ndarray, list[dict[str, float]], list[dict[str, object]]]:
        scores = np.zeros(len(pop), dtype="float64")
        metric_rows: list[dict[str, float]] = []
        payload_rows: list[dict[str, object]] = []
        for idx, chromosome in enumerate(pop):
            fitness, metrics, payload = evaluator(chromosome)
            scores[idx] = float(fitness)
            metric_rows.append(metrics)
            payload_rows.append(payload)
        return scores, metric_rows, payload_rows

    def _select_parent(scores: np.ndarray) -> np.ndarray:
        candidates = rng.choice(len(scores), size=max(2, cfg.tournament_size), replace=False)
        winner = candidates[np.argmax(scores[candidates])]
        return population[winner].copy()

    def _crossover(left: np.ndarray, right: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        mask = rng.random(dimensions) < 0.5
        child_a = np.where(mask, left, right)
        child_b = np.where(mask, right, left)
        return child_a, child_b

    def _mutate(child: np.ndarray) -> np.ndarray:
        child = child.copy()
        bit_mask = rng.random(dimensions) < cfg.mutation_rate
        if bit_mask.any():
            child[bit_mask] = np.clip(child[bit_mask] + rng.normal(0.0, cfg.mutation_scale, size=int(bit_mask.sum())), 0.0, 1.0)
        return child

    while generations_run < cfg.generations:
        scores, metric_rows, payload_rows = _evaluate_population(population)
        order = np.argsort(scores)[::-1]
        population = population[order]
        scores = scores[order]
        metric_rows = [metric_rows[idx] for idx in order]
        payload_rows = [payload_rows[idx] for idx in order]

        if float(scores[0]) > best_fitness:
            best_fitness = float(scores[0])
            best_chromosome = population[0].copy()
            best_metrics = metric_rows[0]
            best_payload = payload_rows[0]
            stale_generations = 0
        else:
            stale_generations += 1

        generations_run += 1
        if stale_generations >= cfg.patience:
            break

        next_population = [population[idx].copy() for idx in range(elite_count)]
        while len(next_population) < cfg.population:
            parent_a = _select_parent(scores)
            parent_b = _select_parent(scores)
            child_a, child_b = _crossover(parent_a, parent_b)
            next_population.append(_mutate(child_a))
            if len(next_population) < cfg.population:
                next_population.append(_mutate(child_b))
        population = np.asarray(next_population[: cfg.population], dtype="float64")

    return GAResult(
        chromosome=best_chromosome,
        fitness=best_fitness,
        metrics=best_metrics,
        payload=best_payload,
        generations_run=generations_run,
    )


def decode_feature_subset(chromosome: np.ndarray, feature_names: list[str]) -> list[str]:
    if not feature_names:
        return []
    mask = chromosome[: len(feature_names)] >= 0.5
    selected = [name for name, keep in zip(feature_names, mask, strict=False) if keep]
    return selected or feature_names[: min(6, len(feature_names))]


def decode_weight_map(chromosome: np.ndarray, factor_names: list[str]) -> dict[str, float]:
    feature_count = len(factor_names)
    weights_raw = chromosome[feature_count : feature_count * 2] if len(chromosome) >= feature_count * 2 else chromosome[:feature_count]
    if weights_raw.size < feature_count:
        weights_raw = np.pad(weights_raw, (0, feature_count - weights_raw.size), constant_values=0.5)
    mask = chromosome[:feature_count] >= 0.5
    transformed = (weights_raw[:feature_count] - 0.5) * 2.0
    transformed = np.where(mask, transformed, 0.0)
    if float(np.abs(transformed).sum() or 0.0) <= 0.0:
        transformed = np.ones(feature_count, dtype="float64")
    transformed = transformed / float(np.abs(transformed).sum() or 1.0)
    return {name: float(weight) for name, weight in zip(factor_names, transformed, strict=False)}


def ga_summary_json(result: GAResult, *, selected: list[str], extra: dict[str, object] | None = None) -> str:
    payload = {
        "fitness": result.fitness,
        "metrics": result.metrics,
        "selected": selected,
        "generationsRun": result.generations_run,
        **(extra or {}),
    }
    return json.dumps(payload, default=float)
