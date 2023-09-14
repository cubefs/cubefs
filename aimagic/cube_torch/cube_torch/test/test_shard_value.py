import multiprocessing


def worker_func(counter, comparison_value):
    counter.value += 1
    result = counter.value + comparison_value.value

    if result > comparison_value.value:
        print(f"Result ({result}) is greater than comparison value ({comparison_value.value})")
    elif result < comparison_value.value:
        print(f"Result ({result}) is less than comparison value ({comparison_value.value})")
    else:
        print(f"Result ({result}) is equal to comparison value ({comparison_value.value})")


if __name__ == "__main__":
    counter = multiprocessing.Value("i", 0)  # 创建一个整数类型的共享值，并初始化为0
    comparison_value = multiprocessing.Value("i", 10)  # 创建另一个整数类型的共享值，并初始化为10
    processes = []
    for _ in range(5):
        p = multiprocessing.Process(target=worker_func, args=(counter, comparison_value))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    print(f"Counter value in the main process: {counter.value}")
