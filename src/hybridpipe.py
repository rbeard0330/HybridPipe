"""
Basic usage:
>> pipe = HybridPipe()
>> pipe.register_resource(list_of_data)
>> pipe.wrap_producer(my_async_processing_function)(arg1, arg2)
[OR
>> prod = pipe.wrap_producer(my_async_processing_function)
>> prod(arg1, arg2)
OR
>> prod = pipe.wrap_producer(my_async_processing_function)
>> prod(arg1)(arg2)
OR
>> prod = pipe.wrap_producer(my_async_processing_function)
>> prod(arg1)
>> # Code to calculate arg2
>> prod(arg2)
END OR]
>> pipe.wrap_consumer(my_sync_func)(arg1, kwarg1=True)
>> with open("output.txt", "w") as f:
>> ... pipe.register_output(f.write)
>> ... pipe.execute()

TO DOS:
Stage-specific error handlers
Permit chaining of pipelines
Allow injected work functions to fork into multiple outputs
"""


from queue import Queue as SyncQueue
from asyncio import Queue as AsyncQueue
import queue as sync_queue
import asyncio
import inspect
import enum
from functools import partial
import functools
import threading
import concurrent.futures
import logging
import datetime
import time

LOG_DIRECTORY = "logs/"

# ERROR_TIMEOUT is intended to prevent locking in error states
ERROR_TIMEOUT = 3
# LOOP_TIMEOUT determines how long to wait in loop conditions
LOOP_TIMEOUT = 0.1


class HybridPipeEnum(enum.Enum):
    RESOURCE_DATA_PLACEHOLDER = enum.auto()
    MISSING_ARG_PLACEHOLDER = enum.auto()
    CONTINUE_LOOP_VALUE = enum.auto()


class _PipeSegment():
    INTERNAL_KW_ARGS = [
            "pipeline_input_generator",
            "pipeline_error_queue_func",
            "pipeline_put_result_func",
            "pipeline_task_done_func"]
    CONFIG_PROPERTIES = [
        "workers"
    ]
    DEFAULT_CONFIG = {
        "workers": 1
    }

    def __init__(self, func, config_dict=None, logger=None):
        self.func = func
        self.sig = inspect.signature(func, follow_wrapped=True)
        self.stored_args = []
        self.stored_kwargs = {}
        self.logger = logger or logging.getLogger(name=__name__)
        config_dict = config_dict or self.DEFAULT_CONFIG
        self.configure(**config_dict)

    def __call__(self, *args, _pipeline_reset=False, **kwargs):
        """Call object to apply static args to function. Returns updated
        callable.

        If called multiple times without reset flag, new positional arguments
        will be added to previously provided positional arguments. If too many
        positional arguments are provided, it is a TypeError. New keyword
        arguments override previously provided keyword arguments.

        Reset flag deletes stored arguments.

        Wrapped functions will be provided with pipeline data as an argument.
        This argument will be provided in this order of priority:
        1. To replace all instances of HybridPipeEnum.RESOURCE_DATA_PLACEHOLDER
        2. As a keyword argument "_pipeline_data"
        3. As the first positional argument
        """

        if _pipeline_reset:
            self.args = []
            self.kwargs = {}
        tentative_args = self.stored_args + [*args]
        tentative_kwargs = {**self.stored_kwargs, **kwargs}

        # Throws TypeError if invalid args
        self.sig.bind_partial(*tentative_args, **tentative_kwargs)
        self.stored_args = tentative_args
        self.stored_kwargs = tentative_kwargs
        self.logger.debug(f"stored {[*args]} and {kwargs}")

        return self

    def configure(self, **kwargs):
        """"if args:
            raise TypeError("All configuration options should be specified by"
                            + f" keyword: {self.CONFIG_PROPERTIES}")"""
        for key, value in kwargs.items():
            if key not in self.CONFIG_PROPERTIES:
                raise KeyError(f"{key} is not a valid configuration property."
                    + f" Valid properties are: {self.CONFIG_PROPERTIES}")
            else:
                setattr(self, key, value)

    def _unwrap(self, **pipeline_kwargs):
        assert len(pipeline_kwargs) == len(self.INTERNAL_KW_ARGS),\
                f"Expected args matching: {self.INTERNAL_KW_ARGS}"
        _context = {
            "func": self.func,
            "args": self.stored_args,
            "kwargs": {**self.stored_kwargs, **pipeline_kwargs}
        }
        self.logger.debug(
            f"unwrapping {self.func} with args {self.stored_args} and "
            f"kwargs {self.stored_kwargs} and {pipeline_kwargs}")
        if inspect.iscoroutinefunction(self.func):
            @functools.wraps(self.func)
            async def prepped_func():
                nonlocal _context
                return await self.func(*_context["args"], **_context["kwargs"])
        else:
            @functools.wraps(self.func)
            def prepped_func():
                nonlocal _context
                return self.func(*_context["args"], **_context["kwargs"])

        return prepped_func


class HybridPipe():
    """Main class. Chains async and sync functions in a data pipeline.

    Methods:
    __init__:
        Configures pipeline. Accepts custom logger and max queue depth
    register_resource:
        Data source for pipeline. Currently must be an iterable.
    wrap_producer:
        Provide async data processing function to be wrapped to interface with
        pipeline. Returns an object that can be called with non-pipeline
        arguments.
    inject_producer:
        Provide async data processing function to be injected with
        pipeline-interaction functions. Returns an object that can be called
        with non-pipeline arguments.
    wrap_consumer:
        As above, but must be a sync function
    inject_consumer:
        As above, but must be a sync function
    register_output:
        Provide a function to be called with each output.
    execute:
        Run pipeline
    """

    def __init__(
            self, max_size=0,
            default_config=None,
            logger=None, log_level=logging.WARNING,
            _pipe_id=0):

        self.pipe_id = _pipe_id

        self.prod_error_queue = None
        self.cons_error_queue = None
        self.producers = []
        self.consumers = []
        self.resource = None
        self.output_func = None
        self.output_data = None
        self.output_pipe = None
        self.global_error_handler = None
        self.default_error_handler = _PipeSegment(_default_error_handler)
        self.default_config = default_config

        if logger is not None:
            self.logger = logger
        else:
            self.logger = logging.getLogger(name=__name__)
            handlers = [*self.logger.handlers]
            # inelegant, since it would remove a handler that wasn't installed
            # by another instance of the __init__ method
            for handler in handlers:
                handler.close()
                self.logger.removeHandler(handler)
            self.logger.setLevel(logging.DEBUG)
            time = str(datetime.datetime.now())\
                .replace(" ", "-")\
                .replace(".", "-")\
                .replace(":", "-")
            log_format = logging.Formatter(
                "{asctime}: {name} - {levelname}: "
                + str(self.pipe_id)
                + ": {message}", style="{")
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(log_format)
            console_handler.setLevel(log_level)
            file_handler = logging.FileHandler(
                LOG_DIRECTORY + f"{time}HybridPipe.log", mode="w+")
            file_handler.setFormatter(log_format)
            file_handler.setLevel(logging.DEBUG)
            self.logger.addHandler(console_handler)
            self.logger.addHandler(file_handler)

    def register_resource(self, iterable):
        """Connect the pipeline to an iterable data source.

        The pipeline's resource will be used to provide input to the producer
        function.

        params:
        iterable            Generator or iterable
        """

        if self.resource:
            raise TypeError(
                "A resource has already been "
                + f"registered: {self.resource}"
            )

        self.logger.info(f"registered {iterable} as a resource")

        def resource_generator():
            for data in iterable:
                yield data
        self.resource = resource_generator
        return True

    def register_output(self, output_func):
        self.output_func = output_func
        self.logger.info(f"registered {output_func} as output")

    def register_global_error_handler(self, handler, config_dict=None):
        """Register simple error handler, which will be called for all errors.

        Params:
        handler: (sync func) Will be called on all errors that are passed to
        the error queue by consumers or producers.
        config_dict: (dict, optional) Keyword configuration properties for
        function.

        Returns:
        Callable object that can be called with static parameters.
        """

        if self.global_error_handler:
            raise TypeError(
                "A global error handler has already been "
                + f"registered: {self.global_error_handler}"
            )
        if inspect.iscoroutinefunction(func):
            raise ValueError(
                f"Global error handler must not be awaitable: {func}")

        self.logger.info(f"registered {handler} as a global error handler")

        @functools.wraps(handler)
        def wrapped_handler(
                *args,  # Static args
                pipeline_input_generator,
                pipeline_task_done_func,
                **kwargs):  # <- static args
            for error in pipeline_input_generator():
                handler(error)
                pipeline_task_done_func()

        new_handler = _PipeSegment(
            wrapped_handler,
            config_dict=(config_dict or self.default_config))
        self.global_error_handler = new_handler
        return new_handler

    def wrap_producer(self, awaitable, config_dict=None):
        """Wrap producer function.

        Params:

        awaitable       (awaitable) producer co-routine

        Returns:
        callable object to be called with static parameters
        """

        if not inspect.iscoroutinefunction(awaitable):
            raise ValueError(f"Producer must be awaitable: {awaitable}")

        @functools.wraps(awaitable)
        async def wrapped_producer(
            *args,  # Static args
            pipeline_input_generator,
            pipeline_error_queue_func,
            pipeline_put_result_func,
            pipeline_task_done_func,
            # Static arguments
            **kwargs
        ):
            self.logger.debug(f"async task {awaitable.__name__} starting")
            async for data in pipeline_input_generator():
                self.logger.debug(f"{awaitable} took {data} from the queue")
                modified_args, modified_kwargs =\
                    self.modify_arguments(data, args, kwargs)

                try:
                    result = await awaitable(
                        *modified_args, **modified_kwargs)
                    pipeline_task_done_func()
                    await _await_ambiguous_function(
                        pipeline_put_result_func, result)
                except (asyncio.QueueFull, sync_queue.Full) as e:
                    self.logger.exception(
                        f"queue error occurred and wasn't handled")
                    raise e
                except Exception as e:
                    self.logger.warning(
                        f"exception passed to handler", exc_info=True)
                    pipeline_task_done_func()
                    error_dict = {
                        "data": data,
                        "exception": e,
                        "args": modified_args,
                        "kwargs": modified_kwargs
                    }
                    await pipeline_error_queue_func(error_dict)
            self.logger.debug(f"async task {awaitable.__name__} complete")
            return f"Wrapped producer task {awaitable.__name__} completed"

        new_segment = _PipeSegment(
            wrapped_producer,
            config_dict=(config_dict or self.default_config))
        self.producers.append(new_segment)
        return new_segment

    def inject_producer(self, awaitable, config_dict=None):
        """Inject producer function.

        Injected functions receive 4 keyword arguments:
        pipeline_input_generator:
            an async generator over the incoming data from the pipeline
        pipeline_error_queue_func:
            a function to be awaited with exception information to send to
            error queue
        pipeline_put_result_func:
            a function to be awaited with processed data for next stage of
            pipeline
        pipeline_task_done_func:
            a function to be called after processing each input (including
            exceptions!)

        Params:

        awaitable       (awaitable) producer co-routine

        Returns:
        callable object to be called with static parameters
        """

        if not inspect.iscoroutinefunction(awaitable):
            raise ValueError(f"Producer must be awaitable: {awaitable}")

        @functools.wraps(awaitable)
        async def injected_producer(
            *args,  # Static args
            pipeline_input_generator,
            pipeline_error_queue_func,
            pipeline_put_result_func,
            pipeline_task_done_func,
            # Static arguments
            **kwargs
        ):
            return await awaitable(
                *args,
                pipeline_input_generator=pipeline_input_generator,
                pipeline_error_queue_func=pipeline_error_queue_func,
                pipeline_put_result_func=pipeline_put_result_func,
                pipeline_task_done_func=pipeline_task_done_func,
                **kwargs)

        new_segment = _PipeSegment(
            injected_producer,
            config_dict=(config_dict or self.default_config))
        self.producers.append(new_segment)
        return new_segment

    def wrap_consumer(self, func, config_dict=None):
        """Wrap consumer function.

        Params:

        func          (sync function) consumer function

        Returns:
        callable object to be called with static parameters
        """

        if inspect.iscoroutinefunction(func):
            raise ValueError(f"Consumer must not be awaitable: {func}")

        @functools.wraps(func)
        def wrapped_consumer(
            *args,  # Static args
            pipeline_input_generator,
            pipeline_error_queue_func,
            pipeline_put_result_func,
            pipeline_task_done_func,
            # Static arguments
            **kwargs
        ):
            for data in pipeline_input_generator():
                # Insert data into argument list
                self.logger.debug(f"{func} took {data} from the queue")
                modified_args, modified_kwargs =\
                    self.modify_arguments(data, args, kwargs)
                try:
                    result = func(*modified_args, **modified_kwargs)
                    pipeline_task_done_func()
                    pipeline_put_result_func(result)
                except sync_queue.Full as e:
                    self.logger.exception(
                        f"queue error occurred and wasn't handled")
                except Exception as e:
                    self.logger.warning(
                        f"exception passed to handler", exc_info=True)
                    error_dict = {
                        "data": data,
                        "exception": e,
                        "args": modified_args,
                        "kwargs": modified_kwargs
                    }
                    pipeline_task_done_func()
                    pipeline_error_queue_func(error_dict)
            self.logger.info(f"sync task {func.__name__} complete")
            return f"Wrapped consumer task {func.__name__} completed"

        new_segment = _PipeSegment(
            wrapped_consumer,
            config_dict=(config_dict or self.default_config))
        self.consumers.append(new_segment)
        return new_segment

    def inject_consumer(self, func, config_dict=None):
        """Inject consumer function.

        Params:

        func          (sync function) consumer function

        Returns:
        callable object to be called with static parameters
        """
        if inspect.iscoroutinefunction(func):
            raise ValueError(f"Consumer must not be awaitable: {func}")

        @functools.wraps(func)
        def injected_consumer(
            *args,  # Static args
            pipeline_input_generator,
            pipeline_error_queue_func,
            pipeline_put_result_func,
            pipeline_task_done_func,
            # Static arguments
            **kwargs
        ):
            return func(
                *args,
                pipeline_input_generator=pipeline_input_generator,
                pipeline_error_queue_func=pipeline_error_queue_func,
                pipeline_put_result_func=pipeline_put_result_func,
                pipeline_task_done_func=pipeline_task_done_func,
                **kwargs)

        new_segment = _PipeSegment(
            injected_consumer,
            config_dict=(config_dict or self.default_config))
        self.consumers.append(new_segment)
        return new_segment

    def modify_arguments(self, data, args, kwargs):
        """Insert data into appropriate place in args or kwargs.

        Returns modified *args and **kwargs.
        """

        modified_args = list(args)
        placeholder = HybridPipeEnum.RESOURCE_DATA_PLACEHOLDER
        kw_placeholder = "_pipeline_data"
        if (
            (found_arg := placeholder in args)
            or (found_kwarg := placeholder in kwargs.values())
        ):
            if found_arg:
                while placeholder in modified_args:
                    modified_args[modified_args.index(placeholder)] =\
                        data
            if found_kwarg:
                modified_kwargs = {
                    key: data if val == placeholder else val
                    for (key, val) in kwargs.items()
                }
        elif kw_placeholder in kwargs:
            modified_kwargs = kwargs
            modified_kwargs[kw_placeholder] = data
        else:
            modified_args = [data] + modified_args
            modified_kwargs = kwargs

        return modified_args, modified_kwargs

    # ------------Execution routines---------------

    def execute(self):
        "Assemble pipeline and run main coroutine."

        if not self.resource:
            raise TypeError("""A resource must be registered before the
                            pipeline can run. Call register_resource.""")
        if not self.producers:
            raise TypeError("""A producer must be registered before the
                            pipeline can run. Call wrap_producer or
                            inject_producer.""")
        if not self.consumers:
            raise TypeError("""A consumer must be registered before the
                            pipeline can run. Call wrap_consumer or
                            inject_consumer.""")
        if self.output_func is None:
            self.output_data = []
            self.output_func = self.output_data.append

        # Add specific error handlers later
        if self.global_error_handler is None:
            self.global_error_handler = self.default_error_handler

        try:
            asyncio.run(self._run_pipeline(), debug=True)
        except Exception as e:
            print(e)

        if self.output_data:
            return self.output_data

    async def _run_pipeline(self):
        async_tasks, sync_tasks, segments = await self._get_my_tasks()

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(sync_tasks)) as pool:
            self.logger.info(f"starting sync consumer tasks: {sync_tasks}")
            thread_futures = [
                    asyncio.get_running_loop().run_in_executor(pool, task)
                    for task in sync_tasks
                ]
            self.logger.info(
                f"starting async producer tasks: {async_tasks}")
            try:
                results = await asyncio.gather(
                *thread_futures,
                *async_tasks,
                return_exceptions=False)
            except Exception as e:
                self.logger.exception(e)
        self.logger.info(results)
        await asyncio.sleep(.1)

    # ------------Assemble tasks---------------

    async def _get_my_tasks(self, segments=1):

        resource_exhausted = asyncio.Event()
        resource_queue = AsyncQueue()
        pipeline = SyncQueue()
        loader_task = self._loader_coro(
            self.resource, resource_queue, resource_exhausted)
        self.logger.debug(f"loader task created: {loader_task}")

        # TOOD: Add specific error handlers
        global_error_queue = SyncQueue()
        global_error_queue_sync_put = partial(
            global_error_queue.put, timeout=ERROR_TIMEOUT)
        global_error_queue_async_put =\
            _get_awaitable_sync_put(global_error_queue, ERROR_TIMEOUT)
        error_processing_complete = threading.Event()

        # Create producer tasks
        interproducer_qs = [
            AsyncQueue() for _ in range(len(self.producers) - 1)]
        if interproducer_qs:
            self.logger.debug(f"created an interproducer queue of "
                              f"length {len(interproducer_qs)}")
        producer_done_signals = (
            [resource_exhausted]
            + [asyncio.Event() for _ in range(len(self.producers))]
            + [threading.Event()]
        )
        producer_func_sets = [{
            "pipeline_input_generator": _build_async_generator(
                resource_queue.get, producer_done_signals[1].is_set),
            "pipeline_error_queue_func": global_error_queue_async_put,
            # None is placeholder that would be overwritten later
            "pipeline_put_result_func": (
                interproducer_qs[0].put if interproducer_qs else None),
            "pipeline_task_done_func": resource_queue.task_done,
        }]

        for ix, producer in enumerate(self.producers[1:-1]):
            producer_func_sets.append({
                "pipeline_input_generator": _build_async_generator(
                    interproducer_qs[ix].get,
                    producer_done_signals[ix + 2].is_set),
                "pipeline_error_queue_func": global_error_queue_async_put,
                "pipeline_put_result_func": interproducer_qs[ix + 1].put,
                "pipeline_task_done_func": interproducer_qs[ix].task_done
            })

        if interproducer_qs:
            producer_func_sets.append({
                "pipeline_input_generator": _build_async_generator(
                    interproducer_qs[-1].get,
                    producer_done_signals[-2].is_set),
                "pipeline_error_queue_func": global_error_queue_async_put,
                "pipeline_task_done_func": interproducer_qs[-1].task_done,
            })

        producer_func_sets[-1]["pipeline_put_result_func"] =\
            _get_awaitable_sync_put(pipeline, ERROR_TIMEOUT)

        assert len(self.producers) == len(producer_func_sets)

        async_tasks = []
        self.logger.debug(
            f"beginning to unwrap {len(self.producers)} producers")
        for ix, producer in enumerate(self.producers):
            async_tasks += [
                producer._unwrap(**producer_func_sets[ix])()
                for _ in range(producer.workers)
            ]
        async_tasks += [
            loader_task,
            self._producer_signal_monitor(
                [resource_queue, *interproducer_qs],
                producer_done_signals)
        ]

        self.logger.debug(f"async tasks unwrapped: {async_tasks}")

        # -----------Create consumer tasks-------------------
        interconsumer_qs = [
            SyncQueue() for _ in range(len(self.consumers) - 1)
        ]
        if interconsumer_qs:
            self.logger.debug(f"created an interconsumer queue of "
                              f"length {len(interconsumer_qs)}")
        consumer_done_signals = [
                threading.Event() for _ in range(len(self.consumers))
            ]

        consumer_func_sets = [{
            "pipeline_input_generator": _build_sync_generator(
                _wrapped_sync_get(pipeline, LOOP_TIMEOUT),
                consumer_done_signals[0].is_set),
            "pipeline_error_queue_func": global_error_queue_sync_put,
            # None is placeholder that would be overwritten later
            "pipeline_put_result_func": (
                partial(interconsumer_qs[0].put, timeout=ERROR_TIMEOUT)
                if interconsumer_qs
                else None),
            "pipeline_task_done_func": pipeline.task_done
        }]

        for ix, consumer in enumerate(self.consumers[1:-1]):
            consumer_func_sets.append({
                "pipeline_input_generator": _build_sync_generator(
                    _wrapped_sync_get(interconsumer_qs[ix], LOOP_TIMEOUT),
                    consumer_done_signals[ix + 1].is_set
                ),
                "pipeline_error_queue_func": global_error_queue_sync_put,
                "pipeline_put_result_func":
                    partial(interconsumer_qs[ix + 1].put,
                            timeout=ERROR_TIMEOUT),
                "pipeline_task_done_func": interconsumer_qs[ix].task_done
            })

        if interconsumer_qs:
            consumer_func_sets.append({
                "pipeline_input_generator": _build_sync_generator(
                    _wrapped_sync_get(interconsumer_qs[-1], LOOP_TIMEOUT),
                    consumer_done_signals[-1].is_set
                ),
                "pipeline_error_queue_func": global_error_queue_sync_put,
                "pipeline_task_done_func": interconsumer_qs[-1].task_done
            })

        consumer_func_sets[-1]["pipeline_put_result_func"] = self.output_func

        assert len(self.consumers) == len(consumer_func_sets)

        self.logger.debug(
            f"beginning to unwrap {len(self.consumers)} consumers")
        sync_tasks = []
        for ix, consumer in enumerate(self.consumers):
            sync_tasks += [
                consumer._unwrap(**consumer_func_sets[ix])
                for _ in range(consumer.workers)
            ]

        @functools.wraps(self._consumer_signal_monitor)
        def consumer_monitor():
            return self._consumer_signal_monitor(
                [pipeline, *interconsumer_qs, global_error_queue],
                [
                    producer_done_signals[-1],
                    *consumer_done_signals,
                    error_processing_complete
                ])

        sync_tasks.append(consumer_monitor)
        sync_tasks.append(
            self.global_error_handler._unwrap(
                pipeline_input_generator=_build_sync_generator(
                    _wrapped_sync_get(global_error_queue, LOOP_TIMEOUT),
                    error_processing_complete.is_set),
                pipeline_task_done_func=global_error_queue.task_done,
                pipeline_put_result_func=None,
                pipeline_error_queue_func=None
            )
        )
        self.logger.debug(f"sync tasks unwrapped: {sync_tasks}")
        if self.output_pipe:
            self.logger.info("pulling tasks from another HybridPipe")
            new_p_tasks, new_c_tasks, new_segments =\
                self.output_pipe._get_my_tasks()
            async_tasks += new_p_tasks
            sync_tasks += new_c_tasks
            segments += new_segments

        # For debugging
        queue_dict = {
            "resource": resource_queue,
            "producer": interproducer_qs,
            "pipeline": pipeline,
            "consumer": interconsumer_qs
        }
        self.queues = queue_dict

        return async_tasks, sync_tasks, segments

    # -------------Helper routines---------------------------

    async def _loader_coro(self, resource, a_queue, done_sig):
        for value in resource():
            self.logger.debug(f"loading {value} to queue from resource")
            await a_queue.put(value)
        done_sig.set()
        self.logger.info(
            f"async loader task complete.  Queue size: {a_queue.qsize()}")
        return "Loader coroutine completed"

    async def _producer_signal_monitor(self, queues, signals):
        self.logger.debug("producer signal monitor started")
        resource_signal = signals.pop(0)
        async_done_signal = signals.pop()
        assert len(queues) == len(signals)
        await resource_signal.wait()
        self.logger.debug("resource exhausted signal")
        for ix, queue in enumerate(queues):
            await queue.join()
            self.logger.debug(f"producer queue {ix} has joined")
            signals[ix].set()
            await asyncio.sleep(0.1)
        self.logger.info("all producer queues closed")
        async_done_signal.set()  # thread-safe signal
        self.logger.debug("set thread-safe signal from producer queue")
        assert all(map(lambda s: s.is_set, signals))
        return "Producer signal monitor completed"

    def _consumer_signal_monitor(self, queues, signals):
        self.logger.debug("consumer signal monitor started")
        producer_signal = signals.pop(0)
        assert len(signals) == len(queues)
        producer_signal.wait()
        self.logger.debug("received signal from producers")
        for ix, queue in enumerate(queues[:-1]):
            queue.join()
            self.logger.debug(f"consumer queue {ix} has joined")
            signals[ix].set()
            time.sleep(0.1)
        self.logger.info("all consumer queues closed")
        queues[-1].join()  # Error queue
        self.logger.debug(f"Error processing complete")
        signals[-1].set()
        assert all(map(lambda s: s.is_set, signals))
        self.logger.info(
            "_consumer_signal_monitor reports all sync tasks done")
        return "Consumer signal monitor completed"


def _build_async_generator(get_data_func,
                           termination_func=lambda _: False,
                           timeout=0.1):
    """Create an async generator to loop over the values provided by input
    function.

    Inputs:
    get_data_func       (function/awaitable) Function to call to get next value
    termination_func    (function/awaitable, optional) Function that
                         terminates iteration. Infinite loop if not provided.
    timeout             (int/float, optional) Time in seconds to wait for
                         input before checking for termination. Default is 0.1
                         secs

    Returns:
    Generator
    """

    async def data_generator():
        while not await _await_ambiguous_function(termination_func):
            try:
                data = await asyncio.wait_for(
                    _await_ambiguous_function(get_data_func), timeout)
                yield data
            except asyncio.TimeoutError:
                continue

    return data_generator


def _build_sync_generator(get_data_func,
                          termination_func=lambda _: False):
    """Create a generator to loop over the values provided by input function.

    Inputs:
    get_data_func       (function) Function to call to get next value
    termination_func    (function, optional) Function that
                         terminates iteration.  Infinite loop if not provided.

    Returns:
    Generator
    """

    def data_generator():
        while not termination_func():
            value = get_data_func()
            if value is not HybridPipeEnum.CONTINUE_LOOP_VALUE:
                yield value

    return data_generator


async def _await_ambiguous_function(f, *args, **kwargs):
    "Await f with args if it is a sync function, else just call it."
    if inspect.iscoroutinefunction(f):
        data = await f(*args, **kwargs)
        return data
    return f(*args, **kwargs)


def _wrapped_sync_get(queue, timeout):
    "Return function to wait for queue, but provide a sentinel on timeout."

    @functools.wraps(queue.get)
    def wrapped_get():
        try:
            return queue.get(timeout=timeout)
        except sync_queue.Empty:
            return HybridPipeEnum.CONTINUE_LOOP_VALUE

    return wrapped_get


def _get_awaitable_sync_put(queue, timeout):
    async def _awaitable_put(data):
        return queue.put(data, timeout=timeout)
    return _awaitable_put


def NULL_FUNC():
    pass


if __name__ == "__main__":
    try:
        HybridPipe()
    except Exception as e:
        print("exception!")
        print(e)

def _default_error_handler(
        *, pipeline_input_generator, pipeline_task_done_func, **kwargs):
    for error in pipeline_input_generator():
        raise Exception(
            f"An error occurred and no handler was provided: {error}")
    return "Error handler completed"