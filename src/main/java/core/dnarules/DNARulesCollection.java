package core.dnarules;

import core.BaseSequence;
import java.util.*;

public class DNARulesCollection implements DNARule {

    private static final float ONE_OVER_SQRT_E = 1.0f / (float) Math.sqrt(Math.E);
    private static final float C_N = 1.0f;

    private static final String NO_NAME_RULE_PREFIX = "NO_NAME_RULE_";

    private int noNameRuleCounter;
    protected Map<String, DNARule> rules;

    /**
     * Creates a collection of DNARule that can be added, replaced, and removed.
     */
    public DNARulesCollection() {
        this.rules = new HashMap<>();
        this.noNameRuleCounter = 0;
    }

    public float evalErrorByLimit(BaseSequence seq, float maxError) {
        return evalErrorByLimitByRules(seq, maxError, rules.values());
    }

    /**
     * Sets the rules of this instance to be immutable.
     */
    public void unmodifiable() {
        this.rules = Collections.unmodifiableMap(rules);
    }

    public static float evalErrorByLimitByRules(BaseSequence seq, float maxError, Collection<DNARule> rs) {
        float sumError = 0.0f;
        for (DNARule rule : rs) {
            sumError += rule.evalErrorProbability(seq);
            if (sumError > maxError)
                return normalizeError(sumError);
        }
        return normalizeError(sumError);
    }

    public void addRule(DNARule rule) {
        addOrReplaceRule(NO_NAME_RULE_PREFIX + (noNameRuleCounter++), rule);
    }

    public void addOrReplaceRule(String ruleName, DNARule rule) {
        this.rules.put(ruleName, rule);
    }

    public DNARule removeRule(String ruleName) {
        return this.rules.remove(ruleName);
    }

    public Map<String, DNARule> getRules() {
        return Collections.unmodifiableMap(rules);
    }

    @Override
    public float evalErrorProbability(BaseSequence seq) {
        return evalErrorProbabilityByRules(seq, rules.values());
    }

    public static float evalErrorProbabilityByRules(BaseSequence seq, Collection<DNARule> rs) {
        float totalError = 0.0f;
        for (DNARule rule : rs)
            totalError += rule.evalErrorProbability(seq);

        return normalizeError(totalError);
    }

    public static float normalizeError(float error) {
        return C_N * ((1 + ONE_OVER_SQRT_E) / (1 + (float) Math.exp(-3 * error + 0.5))) - ONE_OVER_SQRT_E;
    }
}
