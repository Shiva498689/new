import functools
import logging
import traceback
import asyncio
from typing import Any

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("fdd_pipeline")

def handle_node_errors(node_name: str):
    """
    Decorator for LangGraph nodes. Catches exceptions, logs the stack trace,
    and updates the state with the error message so the graph can halt gracefully.
    Supports both sync and async functions.
    If state already has an error, bypasses execution.
    """
    def decorator(func):
        # 1. Helper to extract error if it exists
        def _get_existing_error(state: Any) -> Any:
            if isinstance(state, dict):
                return state.get("error")
            else:
                try:
                    return getattr(state, "error", None)
                except Exception:
                    return None

        # 2. Helper to set error on state
        def _set_error(state: Any, error_msg: str) -> Any:
            if isinstance(state, dict):
                state["error"] = error_msg
                return state
            else:
                try:
                    setattr(state, "error", error_msg)
                    return state
                except Exception:
                    # If state is read-only or doesn't support assignment, return dict update
                    return {"error": error_msg}

        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(state: Any, *args, **kwargs):
                existing_err = _get_existing_error(state)
                if existing_err:
                    logger.info(f"Bypassing async node '{node_name}' due to existing error.")
                    return state
                
                try:
                    logger.info(f"Starting async node: {node_name}")
                    result = await func(state, *args, **kwargs)
                    logger.info(f"Successfully completed async node: {node_name}")
                    return result
                except Exception as e:
                    error_msg = f"Pipeline error in {node_name}: {str(e)}"
                    logger.error(f"{error_msg}\n{traceback.format_exc()}")
                    return _set_error(state, error_msg)
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(state: Any, *args, **kwargs):
                existing_err = _get_existing_error(state)
                if existing_err:
                    logger.info(f"Bypassing sync node '{node_name}' due to existing error.")
                    return state
                
                try:
                    logger.info(f"Starting sync node: {node_name}")
                    result = func(state, *args, **kwargs)
                    logger.info(f"Successfully completed sync node: {node_name}")
                    return result
                except Exception as e:
                    error_msg = f"Pipeline error in {node_name}: {str(e)}"
                    logger.error(f"{error_msg}\n{traceback.format_exc()}")
                    return _set_error(state, error_msg)
            return sync_wrapper
    return decorator