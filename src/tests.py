import unittest
import random
import time
import asyncio
import logging
import math

import hybridpipe

logging_status = True
TEST_LOG_LEVEL = logging.ERROR
MODULE_LOG_LEVEL = logging.ERROR
global test_logger
test_logger = logging.getLogger(__name__)
log_format = logging.Formatter(
    "{asctime}: {name} - {levelname}: "
    + "TEST_LOGGER"
    + ": {message}", style="{")
test_logger.setLevel(TEST_LOG_LEVEL)
handler = logging.StreamHandler()
handler.setFormatter(log_format)
test_logger.addHandler(handler)






class TestPipelineBasicFunctions(unittest.TestCase):

    def setUp(self):
        self.workers = 5
        self.log_level = MODULE_LOG_LEVEL
        self.data_lengths = [
            *[i for i in range(1, self.workers + 1)],
            self.workers + 1, 2 * self.workers, 5 * self.workers]

    def test_wrap_fixed_valid(self):
        for n in self.data_lengths:
            self._test_template(n)

    def test_wrap_random_valid(self):
        for n in self.data_lengths:
            self._test_template(n, fixed=False)

    def test_inject_fixed_valid(self):
        for n in self.data_lengths:
            self._test_template(n, wrap=False)

    def test_inject_random_valid(self):
        for n in self.data_lengths:
            self._test_template(n, wrap=False, fixed=False)

    def test_short_wrap_pipeline(self):
        for n in self.data_lengths:
            self._test_template(n, fixed=False, stages=1)

    def test_short_inject_pipeline(self):
        for n in self.data_lengths:
            self._test_template(n, fixed=False, wrap=False, stages=1)

    def test_long_wrap_pipeline(self):
        for n in self.data_lengths:
            self._test_template(n, fixed=False, stages=5)

    def test_long_inject_pipeline(self):
        for n in self.data_lengths:
            self._test_template(n, fixed=False, wrap=False, stages=5)

    def _test_template(
            self, n, wrap=True, fixed=True, valid_data=True, stages=2):
        data = self.get_data(n, fixed, valid_data)
        results, elapsed = self._run_pipeline(data["data"], wrap, stages)

        if logging_status:
            test_logger.debug(
                f"data: {data}, results: {results}, "
                f"elapsed time: {elapsed}")

        if n <= self.workers:
            max_expected_time = (2 * stages + 1.5) * max(data["data"])
        else:
            work_ratio = math.ceil(len(data["data"]) / self.workers)
            max_expected_time =\
                (2 * stages + 0.5) * sum(sorted(data["data"])[-work_ratio:])

        self.assertLess(elapsed, max_expected_time)
        self.assertEqual(set(data["data"]), set(results))
        test_logger.info(f"Passed test with data length {n}, {stages} stages"
                         f", using {'fixed' if fixed else 'random'} data and "
                         f"{'wrapped' if wrap else 'injected'} work functions")

    def get_data(self, data_points, fixed, valid_data):
        good_data = (
            [.1 * (n + 1) for n in range(data_points)]
            if fixed
            else [random.random() for _ in range(data_points)]
        )
        bad_data = [] if valid_data else ["bad_data1", "bad_data2"]
        return {
            "data": bad_data + good_data,
            "bad_data": bad_data,
            "good_data": good_data
        }

    def _run_pipeline(self, data, wrap=True, stages=2):
        base_config = {"workers": self.workers}
        pipe = hybridpipe.HybridPipe(log_level=self.log_level,
                                     default_config=base_config)
        pipe.register_resource(data)
        for n in range(1, stages + 1):
            if wrap:
                pipe.wrap_producer(async_sleep)(n)
                pipe.wrap_consumer(sync_sleep)(n)
            else:
                pipe.inject_producer(injectable_async_sleep)(n)
                pipe.inject_consumer(injectable_sync_sleep)(n)
        start = time.perf_counter()
        results = pipe.execute()
        elapsed = time.perf_counter() - start
        del pipe
        return results, elapsed


class TestPipelineComputations(unittest.TestCase):
    pass


class TestPipelineFixedSize(unittest.TestCase):
    pass


def sync_sleep(t, stage=1):
    if logging_status:
        test_logger.debug(f"stage {stage} sync sleeping for {t}")
    time.sleep(t)
    return t


def injectable_sync_sleep(stage=1, *,
        pipeline_input_generator,
        pipeline_error_queue_func,
        pipeline_put_result_func,
        pipeline_task_done_func):

    for data in pipeline_input_generator():
        if logging_status:
            test_logger.debug(
                f"stage {stage} async sleeping for {data}")
        try:
            time.sleep(data)
            pipeline_task_done_func()
            pipeline_put_result_func(data)
        except Exception as e:
            test_logger.debug(
                f"stage {stage} encountered error {e}", exc_info=True)
            pipeline_task_done_func()
            error_dict = {
                "data": data,
                "exception": e
            }
            pipeline_error_queue_func(error_dict)


async def async_sleep(t, stage=1):
    if logging_status:
        test_logger.debug(f"stage {stage} async sleeping for {t}")
    await asyncio.sleep(t)
    return t


async def injectable_async_sleep(stage=1, *,
        pipeline_input_generator,
        pipeline_error_queue_func,
        pipeline_put_result_func,
        pipeline_task_done_func):

    async for data in pipeline_input_generator():
        if logging_status:
            test_logger.debug(
                f"stage {stage} async sleeping for {data}")
        try:
            await asyncio.sleep(data)
            pipeline_task_done_func()
            await pipeline_put_result_func(data)
        except Exception as e:
            test_logger.debug(
                f"stage {stage} encountered error {e}", exc_info=True)
            pipeline_task_done_func()
            error_dict = {
                "data": data,
                "exception": e
            }
            pipeline_error_queue_func(error_dict)



if __name__ == "__main__":
    unittest.main()


