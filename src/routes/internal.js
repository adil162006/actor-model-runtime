import { Router } from 'express';
import {
  registerWorker,
  heartbeat,
  acquireSingleton,
  renewSingleton,
  drainWorker,
} from '../services/runtime.js';
import { asyncHandler } from '../middleware/errors.js';
const router = Router();
router.post(
  '/workers/register',
  asyncHandler(async (req, res) =>
    res.status(201).json(await registerWorker(req.body)),
  ),
);
router.post(
  '/workers/heartbeat',
  asyncHandler(async (req, res) => res.json(await heartbeat(req.body))),
);
router.post(
  '/workers/drain',
  asyncHandler(async (req, res) =>
    res.json(await drainWorker(req.body.workerId)),
  ),
);
router.post(
  '/singletons/acquire',
  asyncHandler(async (req, res) =>
    res.json({ lease: await acquireSingleton(req.body.workerId) }),
  ),
);
router.post(
  '/singletons/renew',
  asyncHandler(async (req, res) =>
    res.json({ lease: await renewSingleton(req.body.lease) }),
  ),
);
export default router;
