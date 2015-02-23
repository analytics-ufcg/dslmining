package com.example;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.model.PlusAnonymousUserDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

import java.util.Collection;
import java.util.List;

/**
 * A decorate class for a recommender in which a anonymous user can be added and recommendations can be done to this
 * user.
 */
public class WithAnonymousRecommender implements Recommender {

    private final Recommender delegate;
    private final PlusAnonymousUserDataModel model;

    /**
     * Construcor for WithAnonymousRecommender
     *
     * @param builder A builder to the recommender that will be used.
     * @param model   The parsed dataset
     */
    public WithAnonymousRecommender(RecommenderBuilder builder, DataModel model) throws TasteException {
        this.model = new PlusAnonymousUserDataModel(model);
        this.delegate = builder.buildRecommender(this.model);
    }

    /**
     * Recommend items for a anonymous user.
     *
     * @param anonymousUserPrefs The preferences for the anonymous user.
     * @param howMany The amount of item to be recommended.
     * @return A list of recommended items.
     */
    public synchronized List<RecommendedItem> recommend(PreferenceArray anonymousUserPrefs, int howMany) throws TasteException {
        return recommend(anonymousUserPrefs, howMany, null);
    }

    /**
     * Recommend items for a anonymous user.
     *
     * @param anonymousUserPrefs The preferences for the anonymous user.
     * @param howMany The amount of item to be recommended.
     * @param idRescorer A rescorer to recalculate the rating for the profiles.
     * @return A list of recommended items.
     */
    public synchronized List<RecommendedItem> recommend(PreferenceArray anonymousUserPrefs, int howMany, IDRescorer idRescorer) throws TasteException {
        model.setTempPrefs(anonymousUserPrefs);
        List<RecommendedItem> recommendations = recommend(PlusAnonymousUserDataModel.TEMP_USER_ID, howMany, idRescorer);
        model.clearTempPrefs();
        return recommendations;
    }

    /**
     * Recommend N items to a user.
     *
     * @param userID  The user id for the profile will be recommended.
     * @param howMany How many profiles will be recommended
     * @return A list of recommended items.
     */
    @Override
    public List<RecommendedItem> recommend(long userID, int howMany) throws TasteException {
        return delegate.recommend(userID, howMany);
    }

    /**
     * Recommend N items to a user.
     *
     * @param userID     The user id for the profile will be recommended.
     * @param howMany    How many profiles will be recommended
     * @param idRescorer A rescorer to recalculate the rating for the profiles.
     * @return A list of recommended items.
     */
    @Override
    public List<RecommendedItem> recommend(long userID, int howMany, IDRescorer idRescorer) throws TasteException {
        return delegate.recommend(userID, howMany, idRescorer);
    }

    /**
     * Estimate the preference of a user for another user
     * @param userID The id for the user who will receive the recommendation.
     * @param itemID The id for the user who will be recommended.
     * @return The estimated rating from userID1 to userID2
     */
    @Override
    public float estimatePreference(long userID, long itemID) throws TasteException {
        return delegate.estimatePreference(userID, itemID);
    }

    /**
     * Set the preference for the userID1 to the userID2
     * @param userID The id for the user who will rate.
     * @param itemID The id for the user who will receive a rate.
     * @param value The rate from userID1 to userID2
     */
    @Override
    public void setPreference(long userID, long itemID, float value) throws TasteException {
        delegate.setPreference(userID, itemID, value);
    }

    /**
     * Remove the preference for the userID1 to the userID2
     * @param userID The id for the user who will remove the rate.
     * @param itemID The id for the user which the rate will be removed.
     */
    @Override
    public void removePreference(long userID, long itemID) throws TasteException {
        delegate.removePreference(userID, itemID);
    }

    @Override
    public DataModel getDataModel() {
        return model;
    }

    @Override
    public void refresh(Collection<Refreshable> collection) {
        delegate.refresh(collection);
    }
}
