const Apify = require('apify');

const {
    log,
} = Apify.utils;

Apify.main(async () => {
    const input = await Apify.getInput();

    if (!input || typeof input !== 'object') {
        throw new Error('Missing input');
    }

    log.info('Input', input);

    const {
        datasetId,
        fields = [],
        split = {},
    } = input;

    if (!fields || !fields.length) {
        throw new Error('Missing required "fields" parameter');
    }

    if (!datasetId) {
        throw new Error('You must specify "datasetId" parameter');
    }

    const dataset = await Apify.openDataset(datasetId);

    const {
        cleanItemCount,
    } = await dataset.getInfo();

    const aggregate = {};

    for (const field of fields) {
        aggregate[field] = {
            values: new Set(),
            count: 0,
            min: 0,
            max: 0,
            average: 0,
        };
    }

    const splitMap = (str, toSplit, cb) => str.split(toSplit).forEach(cb);

    log.info(`Aggregating data from ${cleanItemCount} items...`);

    await dataset.forEach(async (item) => {
        for (const field of fields) {
            if (Array.isArray(item[field])) {
                item[field].forEach((c) => {
                    if (field in split && typeof c === 'string') {
                        splitMap(c, split[field], (s) => {
                            aggregate[field].values.add(s);
                        });
                    } else {
                        aggregate[field].values.add(c);
                    }
                });
            } else if (field in item) {
                if (field in split && typeof item[field] === 'string') {
                    splitMap(item[field], split[field], (s) => {
                        aggregate[field].values.add(s);
                    });
                } else {
                    aggregate[field].values.add(item[field]);
                }
            }
        }
    });

    log.info('Generating output');

    await Apify.setValue('OUTPUT', Object.entries(aggregate).reduce((acc, [field, set]) => {
        const values = [...set.values.values()];
        const lengths = values.map((s) => {
            if (typeof s === 'string' || Array.isArray(s)) {
                return s.length;
            }
            if (typeof s === 'number') {
                return s;
            }
            return `${s}`.length;
        });
        const min = lengths.reduce((o, i) => (i < o ? i : o), Infinity);
        const max = lengths.reduce((o, i) => (i > o ? i : o), -Infinity);

        return {
            ...acc,
            [field]: {
                values,
                count: values.length,
                min,
                max,
                average: values.length ? Math.round(lengths.reduce((o, v) => o + v, 0) / lengths.length) : 0,
            },
        };
    }, {}));

    log.info('Done, check OUTPUT');
});
