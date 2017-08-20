import glob
import os
import os.path

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from sklearn.ensemble import ExtraTreesRegressor

plt.style.use('seaborn')


GENDER_MAP = {
    0: 'unspecified',
    1: 'male',
    2: 'female',
}


def get_files():
    return glob.glob(os.path.join('/media/data/500px-progressions-processed/',
                                  '*.msg'))


def post_process(series):

    # distance from upload date
    series['index-rel-upload'] = series.index.to_series() - \
        series['meta-uploaded'].iloc[0]
    # distance from upload date
    series['index-rel-data'] = series.index.to_series() - \
        series.index.to_series().iloc[0]

    return series


def load_data(files):
    return {os.path.splitext(os.path.basename(f))[0]:
            post_process(pd.read_msgpack(f))
            for f in files}


def aggregate(data):

    entries = {}
    for photo_id, series in data.items():
        first = series.iloc[0]
        first.index = ['first-{}'.format(c) for c in first.index]
        last = series.iloc[-1]
        last.index = ['last-{}'.format(c) for c in last.index]
        entries[photo_id] = pd.concat([first, last])
    return pd.DataFrame.from_dict(entries, orient='index')


def rotate_tick_labels(ax):
    for tick in ax.get_xticklabels():
        tick.set_rotation(45)
        tick.set_ha('right')


def apply_standard_args(ax, **kwargs):
    if 'rotate_xticks' in kwargs and kwargs['rotate_xticks']:
        rotate_tick_labels(ax)
    if 'title' in kwargs:
        ax.set_title(kwargs['title'])
    if 'xlabel' in kwargs:
        ax.set_xlabel(kwargs['xlabel'])
    if 'ylabel' in kwargs:
        ax.set_ylabel(kwargs['ylabel'])

def uploaded_time_histogram(aggregated, **kwargs):

    fig = plt.figure()
    aggregated['first-meta-uploaded'].dt.hour.value_counts().sort_index().reindex(range(24), fill_value=0).plot.bar(ax=fig.gca())
    fig.gca().set_xlabel("hour of day (UTC)")
    apply_standard_args(fig.gca(), **kwargs)
    return fig


def categorial_distribution(aggregated, item='first-json-user-sex',
                            replace_map=None, **kwargs):

    fig = plt.figure()
    series = aggregated[item]
    if replace_map:
        series = series.replace(GENDER_MAP)
    series.value_counts().sort_index().plot.bar(ax=fig.gca())

    apply_standard_args(fig.gca(), **kwargs)

    return fig


def distance_to_upload_date(aggregated, **kwargs):

    fig = plt.figure()
    sns.distplot(aggregated['first-index-rel-upload'].dt.seconds,
                 kde=False, rug=True,
                 ax=fig.gca())

    apply_standard_args(fig.gca(), **kwargs)

    return fig


def item_per_category(aggregated, item='last-json-highest_rating', **kwargs):

    grouped = aggregated.groupby('last-meta-category')[item].agg(
        ['mean', 'std', 'count']).sort_values('mean')

    fig = plt.figure()
    grouped['mean'].plot.bar(ax=fig.gca(), yerr=grouped['std'])

    fig.gca().set_xticklabels([
        '{} ({})'.format(tick.get_text(), count)
        for tick, count in zip(fig.gca().get_xticklabels(), grouped['count'])])

    apply_standard_args(fig.gca(), **kwargs)

    return fig


def numerical_distribution(aggregated, item='last-json-highest_rating',
                           **kwargs):

    fig = plt.figure()
    sns.distplot(aggregated[item], ax=fig.gca())
    apply_standard_args(fig.gca(), **kwargs)
    return fig


def all_series_progression(data, **kwargs):

    fig = plt.figure()

    for series in data.values():
        series.set_index('index-rel-upload')['json-rating'].plot.line(
            ax=fig.gca(),
            color='black',
            linewidth=3,
            alpha=0.025)

    apply_standard_args(fig.gca(), **kwargs)

    return fig


def aggregated_scatter(aggregated,
                       x='first-json-user-affection',
                       y='last-json-highest_rating',
                       **kwargs):

    fig = plt.figure()
    aggregated.plot.scatter(x=x, y=y, ax=fig.gca())
    apply_standard_args(fig.gca(), **kwargs)
    return fig


def difference_of_followers(data, **kwargs):

    points = []
    for series in data.values():
        points.append((series.iloc[-1]['json-highest_rating'],
                       series.iloc[-1]['json-user-followers_count'] -
                       series.iloc[0]['json-user-followers_count']))
    frame = pd.DataFrame(points,
                         columns=['json-highest_rating',
                                  'delta-followers'])

    fig = plt.figure()

    frame.plot.scatter(x='json-highest_rating', y='delta-followers',
                       ax=fig.gca())

    fig.gca().set_xlabel("highest rating after two days")
    fig.gca().set_ylabel("change in number of followers")

    apply_standard_args(fig.gca(), **kwargs)

    return fig


def feature_importances(aggregated, **kwargs):

    feature_keys = ['json-category', 'meta-latitude',
                    'meta-longitude', 'meta-tags-count', 'meta-title',
                    'meta-description', 'meta-image_width',
                    'meta-image_height', 'json-focal_length',
                    'json-iso', 'json-for_sale', 'json-license_type',
                    'json-collections_count', 'json-has_nsfw_tags',
                    'json-watermark', 'json-user-sex',
                    'json-user-state', 'json-user-about', 'json-user-usertype',
                    'json-user-affection', 'json-user-followers_count',
                    'json-user-analytics_code', 'json-user-contacts',
                    'user-photos', 'user-galleries', 'user-groups']
    feature_keys = ['first-' + key for key in feature_keys]
    features = aggregated[feature_keys].copy()

    # processing
    features['first-meta-title'] = features['first-meta-title'].str.len()
    features['first-meta-description'] = features[
        'first-meta-description'].str.len()
    features['first-json-focal_length'] = pd.to_numeric(
        features['first-json-focal_length'], errors='coerce')
    features['first-json-user-state'] = features[
        'first-json-user-state'].str.len()
    features['time-since-registration'] = aggregated[
        'first-meta-uploaded'] - aggregated[
            'first-json-user-registration_date']
    features['time-since-registration'] = features[
        'time-since-registration'].dt.seconds
    features['first-json-user-about'] = features[
        'first-json-user-about'].str.len()
    features['first-json-user-analytics_code'] = features[
        'first-json-user-about'].astype(bool)

    forest = ExtraTreesRegressor(n_estimators=250)

    forest.fit(features.fillna(features.mean()),
               aggregated['last-json-highest_rating'])
    std = np.std([tree.feature_importances_ for tree in forest.estimators_],
                 axis=0)
    std = pd.Series(std, index=features.columns)
    importances = pd.Series(forest.feature_importances_,
                            index=features.columns)

    importances = importances.sort_values(ascending=False)
    std = std.reindex_like(importances)

    fig = plt.figure()

    importances.plot.bar(ax=fig.gca(), yerr=std)

    apply_standard_args(fig.gca(), **kwargs)

    return fig


def std_eval(data):

    aggregated = aggregate(data)

    figures = []

    # description of the data set
    # figures.append(
    #     uploaded_time_histogram(
    #         aggregated,
    #         title='contained photos uploaded per hour of day (UTC)'))
    # figures.append(
    #     categorial_distribution(
    #         aggregated,
    #         item='last-json-user-sex',
    #         replace_map=GENDER_MAP,
    #         title='contained photos per gender',
    #         rotate_xticks=True))
    # figures.append(
    #     categorial_distribution(
    #         aggregated,
    #         item='last-meta-category',
    #         title='contained photos per category',
    #         rotate_xticks=True))
    # figures.append(
    #     numerical_distribution(
    #         aggregated,
    #         item='last-json-highest_rating',
    #         title='distribution of highest ratings after 2 days'))
    # figures.append(
    #     numerical_distribution(
    #         aggregated,
    #         item='last-meta-tags-count',
    #         title='distribution of the number of tags per photo'))
    # figures.append(
    #     distance_to_upload_date(
    #         aggregated,
    #         title='time delta between upload date and first scraping',
    #         xlabel='seconds'))
    #
    # # comparison per category
    # figures.append(
    #     item_per_category(
    #         aggregated,
    #         item='last-json-highest_rating',
    #         title='mean highest rating after 2 days per category'))
    # figures.append(
    #     item_per_category(
    #         aggregated,
    #         item='last-meta-tags-count',
    #         title='mean number of tags per category'))

    # numerical relations
    # figures.append(
    #     aggregated_scatter(
    #         aggregated,
    #         x='first-json-user-affection',
    #         y='last-json-highest_rating',
    #         xlabel='user affection at upload',
    #         ylabel='highest rating after two days',
    #         title='relation of user affection and highest rating'))
    # figures.append(
    #     aggregated_scatter(
    #         aggregated,
    #         x='first-json-user-followers_count',
    #         y='last-json-highest_rating',
    #         xlabel='followers at upload',
    #         ylabel='highest rating after two days',
    #         title='relation of and follower count and highest rating'))
    # figures.append(
    #     aggregated_scatter(
    #         aggregated,
    #         x='first-json-user-followers_count',
    #         y='first-json-user-affection',
    #         xlabel='followers',
    #         ylabel='affection',
    #         title='relation of followers and affection'))

    # understand rating
    figures.append(
        aggregated_scatter(
            aggregated,
            x='last-json-times_viewed',
            y='last-json-rating',
            xlabel='times viewed',
            ylabel='rating',
            title='relation of view count and rating'))
    figures.append(
        aggregated_scatter(
            aggregated,
            x='last-json-votes_count',
            y='last-json-rating',
            xlabel='votes',
            ylabel='rating',
            title='relation of vote count and rating'))
    figures.append(
        aggregated_scatter(
            aggregated,
            x='last-json-comments_count',
            y='last-json-rating',
            xlabel='comments',
            ylabel='rating',
            title='relation of comments count and rating'))

    # temporal analysis
    # figures.append(
    #     difference_of_followers(
    #         data,
    #         title='highest rating and change in number of followers'))
    # figures.append(
    #     all_series_progression(
    #         data,
    #         title="evolution of rating"))
