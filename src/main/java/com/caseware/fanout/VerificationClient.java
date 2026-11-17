package com.caseware.fanout;

import java.time.Duration;

/**
 * The downstream owned by another team: loads an engagement (~1 minute) and reports its template version.
 *
 * <p>Contract:
 * <ul>
 *   <li>Returns within {@code timeout}; a timeout is reported as {@link VerificationResult.Failure}.</li>
 *   <li>Read-only: a repeated call is harmless (it only costs a capacity slot), so at-least-once is safe.</li>
 *   <li>Reports overload as {@link VerificationResult.Overloaded}, not as a failure.</li>
 * </ul>
 * Unchecked exceptions are tolerated and treated as {@link VerificationResult.Failure}.
 */
@FunctionalInterface
public interface VerificationClient {
    VerificationResult verify(EngagementRef engagement, Duration timeout);
}
