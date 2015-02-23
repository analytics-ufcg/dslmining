package com.example.libimset;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static com.example.libimset.Utils.parseMenWomen;

/**
 * A custom recommender to the libimset dataset.
 */
public class LibimsetRecommender implements Recommender {

    private final DataModel model;
    private final UserSimilarity similarity;
    private final NearestNUserNeighborhood neighborhood;
    private final Recommender delegate;
    private final FastIDSet men;
    private final FastIDSet women;
    private final FastIDSet usersRateMoreMen;
    private final FastIDSet usersRateLessMen;

    /**
     * Constructor for the LibimsetRecommender
     *
     * @param ratingsPath The path to the file containing the ratings.
     * @param gendersPath The path to the file containing the genders.
     * @throws IOException If one of the files do not exists.
     */
    public LibimsetRecommender(String ratingsPath, String gendersPath) throws IOException, TasteException {
        this(new FileDataModel(new File(ratingsPath)), gendersPath);
    }

    /**
     * Constructor for the LibimsetRecommender
     *
     * @param model      The parsed dataset for the libimset.
     * @param genderPath The path to the file containing the genders.
     * @throws IOException If the file for the gender do not exists.
     */
    public LibimsetRecommender(DataModel model, String genderPath) throws TasteException, IOException {
        this.model = model;
        this.similarity = new EuclideanDistanceSimilarity(model);
        this.neighborhood = new NearestNUserNeighborhood(2, similarity, model);
        this.delegate = new GenericUserBasedRecommender(model, neighborhood, similarity);
        FastIDSet[] menAndWomen = parseMenWomen(genderPath);
        this.men = menAndWomen[0];
        this.women = menAndWomen[1];
        this.usersRateMoreMen = new FastIDSet(50000);
        this.usersRateLessMen = new FastIDSet(50000);
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
        return delegate.recommend(userID, howMany, new GenderRescorer(men, women, usersRateMoreMen, usersRateLessMen, userID, model));
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
     * @param userID1 The id for the user who will receive the recommendation.
     * @param userId2 The id for the user who will be recommended.
     * @return The estimated rating from userID1 to userID2
     */
    @Override
    public float estimatePreference(long userID1, long userId2) throws TasteException {
        IDRescorer rescorer = new GenderRescorer(men, women, usersRateMoreMen, usersRateLessMen, userID1, model);
        return (float) rescorer.rescore(userId2, delegate.estimatePreference(userID1, userId2));
    }

    /**
     * Set the preference for the userID1 to the userID2
     * @param userID1 The id for the user who will rate.
     * @param userID2 The id for the user who will receive a rate.
     * @param value The rate from userID1 to userID2
     */
    @Override
    public void setPreference(long userID1, long userID2, float value)
            throws TasteException {
        delegate.setPreference(userID1, userID2, value);
    }

    /**
     * Remove the preference for the userID1 to the userID2
     * @param userID1 The id for the user who will remove the rate.
     * @param userID2 The id for the user which the rate will be removed.
     */
    @Override
    public void removePreference(long userID1, long userID2)
            throws TasteException {
        delegate.removePreference(userID1, userID2);
    }

    @Override
    public DataModel getDataModel() {
        return delegate.getDataModel();
    }

    @Override
    public void refresh(Collection<Refreshable> alreadyRefreshed) {
        delegate.refresh(alreadyRefreshed);
    }
}
