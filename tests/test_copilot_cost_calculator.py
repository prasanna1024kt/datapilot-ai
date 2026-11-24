
import pytest

from copilot.observability.cost_calculator import (
    OpenAICostCalculator,
)


@pytest.fixture
def calculator():
    return OpenAICostCalculator()


def test_embedding_cost(calculator):
    cost = calculator.calculate_model_cost(
        "text-embedding-3-small",
        1_000_000,
    )
    assert cost == 0.02


def test_llm_input_cost(calculator):
    cost = calculator.calculate_model_cost(
        "gpt-4.1",
        1_000_000,
    )
    assert cost == 2.00


def test_llm_output_cost(calculator):
    cost = calculator.calculate_model_cost(
        "gpt-4.1",
        0,
        1_000_000,
    )
    assert cost == 8.00


def test_complete_request_cost(calculator):
    result = calculator.calculate_request_cost(
        embedding_tokens=100_000,
        prompt_tokens=10_000,
        completion_tokens=2_000,
    )

    assert result["embedding_cost_usd"] == 0.002
    assert result["llm_input_cost_usd"] == 0.02
    assert result["llm_output_cost_usd"] == 0.016
    assert result["estimated_cost_usd"] == 0.038
    assert result["currency"] == "USD"


def test_unknown_model_raises_error(calculator):
    with pytest.raises(ValueError):
        calculator.calculate_model_cost(
            "unknown-model",
            100,
        )


@pytest.mark.parametrize("tokens", [-1, 1.5, "100", True])
def test_invalid_token_count(calculator, tokens):
    with pytest.raises(ValueError):
        calculator.calculate_model_cost(
            "gpt-4.1",
            tokens,
        )


def test_zero_tokens_cost_zero(calculator):
    assert calculator.calculate_model_cost(
        "gpt-4.1",
        0,
        0,
    ) == 0.0


def test_dated_gpt41_model_cost(calculator):
    cost = calculator.calculate_request_cost(
        embedding_tokens=100_000,
        prompt_tokens=1_198,
        completion_tokens=569,
        llm_model="gpt-4.1-2025-04-14",
    )

    assert cost["embedding_cost_usd"] == 0.002
    assert cost["llm_input_cost_usd"] == 0.002396
    assert cost["llm_output_cost_usd"] == 0.004552
    assert cost["estimated_cost_usd"] == 0.008948
