import itertools


def generate_combinations(dynamic_option_names, dynamic_option_values, static_options={}):

    combinations = itertools.product(*dynamic_option_values)

    options_list = []
    for combination in list(combinations):
        options = {}
        for key, option_name in enumerate(dynamic_option_names):
            options[option_name] = combination[key]

            # Add static options
            for static_key, static_option in static_options.items():
                options[static_key] = static_option

        options_list.append(options)

    return options_list


if __name__ == '__main__':
    # TODO Move to unit test
    dynamic_option_names = ['time_steps', 'n_epochs', 'dropout', 'kernel_reg', 'n_hidden']
    dynamic_option_values = [
        [5, 10],
        [10000],
        [0.5],
        [None],
        [100],
    ]

    static_options = {
        'activation': 'tanh',
        'batch_size': 10000000000000,
    }

    options_list = generate_combinations(dynamic_option_names, dynamic_option_values, static_options)

    print(options_list)
