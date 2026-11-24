
from dataclasses import dataclass


@dataclass(frozen=True)
class ModelPricing:
    input_per_million: float
    output_per_million: float = 0.0


class OpenAICostCalculator:
    """
    Estimates OpenAI API token costs in USD.
    Rates are USD per million tokens.
    """

    PRICING = {
        "gpt-4.1": ModelPricing(
            input_per_million=2.00,
            output_per_million=8.00,
        ),
        "text-embedding-3-small": ModelPricing(
            input_per_million=0.02,
        ),
    }

    @staticmethod
    def _validate_tokens(value: int, field: str) -> None:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{field} must be a non-negative integer")
        if value < 0:
            raise ValueError(f"{field} must be a non-negative integer")

    def calculate_model_cost(
        self,
        model: str,
        input_tokens: int,
        output_tokens: int = 0,
    ) -> float:
        self._validate_tokens(input_tokens, "input_tokens")
        self._validate_tokens(output_tokens, "output_tokens")

        pricing = self.PRICING.get(model)
        if pricing is None:
            raise ValueError(f"Pricing not configured for model: {model}")

        input_cost = (
            input_tokens / 1_000_000
        ) * pricing.input_per_million

        output_cost = (
            output_tokens / 1_000_000
        ) * pricing.output_per_million

        return round(input_cost + output_cost, 10)

    MODEL_ALIASES = {
        "gpt-4.1-2025-04-14": "gpt-4.1",
    }

    def calculate_model_cost(
        self,
        model: str,
        input_tokens: int,
        output_tokens: int = 0,
    ) -> float:
        self._validate_tokens(input_tokens, "input_tokens")
        self._validate_tokens(output_tokens, "output_tokens")

        pricing_model = self.MODEL_ALIASES.get(model, model)

        pricing = self.PRICING.get(pricing_model)
        if pricing is None:
            raise ValueError(
                f"Pricing not configured for model: {model}"
            )

        input_cost = (
            input_tokens / 1_000_000
        ) * pricing.input_per_million

        output_cost = (
            output_tokens / 1_000_000
        ) * pricing.output_per_million

        return round(input_cost + output_cost, 10)

    
    def calculate_request_cost(
        self,
        embedding_tokens: int,
        prompt_tokens: int,
        completion_tokens: int,
        embedding_model: str = "text-embedding-3-small",
        llm_model: str = "gpt-4.1",
    ) -> dict:
        embedding_cost = self.calculate_model_cost(
            embedding_model,
            embedding_tokens,
        )

        llm_input_cost = self.calculate_model_cost(
            llm_model,
            prompt_tokens,
        )

        llm_output_cost = self.calculate_model_cost(
            llm_model,
            0,
            completion_tokens,
        )

        total_cost = (
            embedding_cost
            + llm_input_cost
            + llm_output_cost
        )

        return {
            "embedding_cost_usd": embedding_cost,
            "llm_input_cost_usd": llm_input_cost,
            "llm_output_cost_usd": llm_output_cost,
            "estimated_cost_usd": round(total_cost, 10),
            "currency": "USD",
            "pricing_basis": "standard_api_token_rates",
        }
